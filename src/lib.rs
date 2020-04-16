use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use gdal::errors::Error;
use gdal::raster::{Buffer, Dataset, Driver};
use gdal::spatial_ref::{CoordTransform, SpatialRef};
use geohash::{self, Coordinate};

use std::io::{Read, Write};

mod coordinate;
pub mod prelude;

pub fn coverage(dataset: &Dataset) -> Result<f64, Error> {
    let (width, height) = dataset.size();
    let mut invalid_pixels = vec![true; width * height];

    // iterate over rasterbands
    for i in 0..dataset.count() {
        let rasterband = dataset.rasterband(i + 1)?;

        // read rasterband data into buffer
        let buffer = rasterband.read_as::<u8>((0, 0),
            (width, height), (width, height))?;

        // iterate over pixels
        for (i, pixel) in buffer.data.iter().enumerate() {
            if *pixel != 0u8 {
                invalid_pixels[i] = false;
            }
        }
    }

    // compute percentage of valid pixels
    let pixel_count = (width * height) as f64;
    let invalid_count = invalid_pixels.iter()
        .filter(|x| **x).count() as f64;

    Ok((pixel_count - invalid_count) / pixel_count)
}

pub fn fill(rasters: &mut Vec<Buffer<u8>>,
        fill_rasters: &Vec<Buffer<u8>>)
        -> Result<(), Box<dyn std::error::Error>> {
    // iterate over pixels
    let size = rasters[0].data.len();
    for i in 0..size {
        if fill_rasters[0].data.len() <= i {
            break;
        }

        // check if rasterband pixel is valid
        let mut valid = false;
        for j in 0..rasters.len() {
            valid = valid || rasters[j].data[i] != 0u8;
        }

        // copy pixels from fill_raster bands
        if !valid {
            for j in 0..rasters.len() {
                rasters[j].data[i] = fill_rasters[j].data[i];
            }
        }
    }

    Ok(())
}

pub fn read<T: Read>(reader: &mut T)
        -> Result<Dataset, Box<dyn std::error::Error>> {
    // read image dimensions
    let width = reader.read_u32::<BigEndian>()? as isize;
    let height = reader.read_u32::<BigEndian>()? as isize;

    // read geo transform
    let mut transform = [0.0f64; 6];
    for i in 0..transform.len() {
        transform[i] = reader.read_f64::<BigEndian>()?;
    }
 
    // read projection
    let projection_len = reader.read_u32::<BigEndian>()?;
    let mut projection_buf = vec![0u8; projection_len as usize];
    reader.read_exact(&mut projection_buf)?;
    let projection = String::from_utf8(projection_buf)?;
 
    // read rasterband count
    let rasterband_count = reader.read_u8()? as isize;

    // initialize dataset - TODO error
    let driver = Driver::get("Mem").unwrap();
    let dataset = driver.create("unreachable",
        width, height, rasterband_count).unwrap();

    dataset.set_geo_transform(&transform).unwrap();
    dataset.set_projection(&projection).unwrap();
 
    // read rasterbands
    let size = (width * height) as usize;
    for i in 0..rasterband_count {
        // read rasterband data
        let mut data = vec![0u8; size];
        reader.read_exact(&mut data)?;

        // write raster to dataset - TODO error
        let buffer = Buffer::new((width as usize, height as usize), data);
        dataset.write_raster(i+1, (0, 0), (width as usize,
            height as usize), &buffer).unwrap();
    }

    Ok(dataset)
}

/*pub fn split(dataset: &Dataset, precision: usize)
        -> Result<Vec<(String, Dataset)>, Error> {
    // compute minimum and maximum latitude and longitude
    let (width, height) = dataset.size();
    let src_width = width as f64;
    let src_height = height as f64;

    let transform = dataset.geo_transform()?;

    let mut xs = Vec::new();
    let mut ys = Vec::new();
    let mut zs = Vec::new();

    xs.push(transform[0]);
    ys.push(transform[3]);
    zs.push(0.0);

    xs.push(transform[0]); 
    ys.push(transform[3] + (src_width * transform[4])
        + (src_height * transform[5]));
    zs.push(0.0);

    xs.push(transform[0] + (src_width * transform[1])
        + (src_height * transform[2]));
    ys.push(transform[3]);
    zs.push(0.0);

    xs.push(transform[0] + (src_width * transform[1])
        + (src_height * transform[2]));
    ys.push(transform[3] + (src_width * transform[4])
        + (src_height * transform[5]));
    zs.push(0.0);

    println!("{:?} {}", transform, dataset.projection());
    let src_spatial_ref = SpatialRef::from_wkt(&dataset.projection())?;
    let dst_spatial_ref = SpatialRef::from_epsg(4326)?;
    let coord_transform = 
        CoordTransform::new(&src_spatial_ref, &dst_spatial_ref)?;

    coord_transform.transform_coords(&mut xs, &mut ys, &mut zs)?;

    let lat_min = ys.iter().cloned().fold(1./0., f64::min);
    let lat_max = ys.iter().cloned().fold(0./0., f64::max);
    let long_min = xs.iter().cloned().fold(1./0., f64::min);
    let long_max = xs.iter().cloned().fold(0./0., f64::max);

    let lat_range = lat_max - lat_min;
    let long_range = long_max - long_min;

    // compute geohash coordinate bounds
    let bounds = coordinate::get_coordinate_bounds(lat_min,
        lat_max, long_min, long_max, precision);

    // open memory driver
    let driver = Driver::get("Mem")?;

    // iterate over bounds
    let mut st_images = Vec::new();
    for bound in bounds {
        // compute pixels for subimage
        let min_y = (((bound.0 - lat_min) / lat_range) * src_height)
            .ceil() as i32;
        let max_y = (((bound.1 - lat_min) / lat_range) * src_height)
            .floor() as i32;

        let min_x = (((bound.2 - long_min) / long_range) * src_width)
            .ceil() as i32;
        let max_x = (((bound.3 - long_min) / long_range) * src_width)
            .floor() as i32;

        // compute geohash - TODO error
        let coordinate = Coordinate{x: bound.3, y: bound.1};
        let geohash = geohash::encode(coordinate, precision).unwrap();
        println!("GEOHASH: {}", geohash);

        // compute image size
        let src_x_offset = min_x.max(0) as isize;
        let src_y_offset = min_y.max(0) as isize;

        let buf_width = (max_x.min(src_width as i32) 
            - min_x.max(0)) as usize;
        let buf_height = (max_y.min(src_height as i32)
            - min_y.max(0)) as usize;

        let dst_x_offset = (0 - min_x).max(0) as isize;
        let dst_y_offset = (0 - min_y).max(0) as isize;

        let dst_width = (max_x - min_x) as isize;
        let dst_height = (max_y - min_y) as isize;

        // initialize new dataset
        let path = format!("/tmp/{}", geohash);
        let output_dataset = driver.create(&path,
            dst_width, dst_height, dataset.count())?;

        // modify transform
        let mut transform = dataset.geo_transform()?;
        transform[0] = transform[0] + (min_x as f64 * transform[1])
            + (min_y as f64 * transform[2]);
        transform[3] = transform[3] + (min_x as f64 * transform[4])
            + (min_y as f64 * transform[5]);

        // TODO - tmp
        let ul_x = transform[0];
        let ul_y = transform[3];

        let lr_x = transform[0] + (dst_width as f64 * transform[1]);
        let lr_y = transform[3] + (dst_height as f64 * transform[5]);

        println!("  x: {} {}", ul_x, lr_x);
        println!("  y: {} {}", lr_y, ul_y);

        output_dataset.set_geo_transform(&transform)?;
        output_dataset.set_projection(&dataset.projection())?;

        // copy rasterband data to new image
        for i in 0..dataset.count() {
            let rasterband = dataset.rasterband(i + 1)?;

            // read rasterband data into buffer
            let buffer = rasterband.read_as::<u8>((src_x_offset, src_y_offset),
                (buf_width, buf_height), (buf_width, buf_height))?;

            // write rasterband data to output dataset
            output_dataset.write_raster(i+1, (dst_x_offset, dst_y_offset),
                (buf_width, buf_height), &buffer)?;
        }

        // add output_dataset to return vector
        st_images.push((geohash, output_dataset))
    }

    Ok(st_images)
}*/

pub fn write<T: Write>(dataset: &Dataset, writer: &mut T)
        -> Result<(), Box<dyn std::error::Error>> {
    // write image dimensions
    let (width, height) = dataset.size();
    writer.write_u32::<BigEndian>(width as u32)?;
    writer.write_u32::<BigEndian>(height as u32)?;

    // write geo transform - TODO error
    let transform = dataset.geo_transform().unwrap();
    for val in transform.iter() {
        writer.write_f64::<BigEndian>(*val)?;
    }

    // write projection
    let projection = dataset.projection();
    writer.write_u32::<BigEndian>(projection.len() as u32)?;
    writer.write(projection.as_bytes())?;

    // write rasterbands
    writer.write_u8(dataset.count() as u8)?;
    for i in 0..dataset.count() {
        // TODO - error
        let rasterband =
            dataset.read_full_raster_as::<u8>(i + 1).unwrap();
        let data = rasterband.data;

        writer.write(&data)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    /*use gdal::raster::{Dataset, Driver};

    use std::io::Cursor;
    use std::path::Path;

    #[test]
    fn image_split() {
        let path = Path::new("examples/L1C_T13TDE_A003313_20171024T175403");

        // read dataset
        let dataset = Dataset::open(path).expect("dataset open");

        // open gtiff driver
        let driver = Driver::get("GTiff").expect("get driver");

        // iterate over geohash split datasets
        for (geohash, dataset) in 
                super::split(&dataset, 4).expect("split dataset") {

            /*for (geohash2, dataset2) in 
                    super::split(&dataset, 4).expect("split dataset") {
                
            }*/

            // copy memory datasets to gtiff files
            dataset.create_copy(&driver, &format!("/tmp/{}", geohash))
                .expect("dataset copy");
        }
    }*/

    /*#[test]
    fn transfer() {
        let path = Path::new("examples/L1C_T13TDE_A003313_20171024T175403");

        // read dataset
        let dataset = Dataset::open(path).expect("dataset open");

        // write dataset to buffer
        let mut buffer = Vec::new();
        super::write(&dataset, &mut buffer).expect("dataset write");

        // read dataset from buffer
        let mut cursor = Cursor::new(buffer);
        let read_dataset = super::read(&mut cursor)
            .expect("dataset read");

        // open gtiff driver
        let driver = Driver::get("GTiff").expect("get driver");
        read_dataset.create_copy(&driver, "/tmp/st-image-transfer")
            .expect("dataset copy");
    }*/
}
