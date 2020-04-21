use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use gdal::errors::Error;
use gdal::raster::{Buffer, Dataset, Driver};
use gdal::spatial_ref::{CoordTransform, SpatialRef};

use std::io::{Read, Write};

pub mod coordinate;
mod test;

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

/*pub fn split(dataset: &Dataset, epsg_code: u32, x_interval: f64,
        y_interval: f64) -> Result<Vec<Dataset>, Error> {
    // initialize transform array and CoordTransform's from dataset
    let transform = dataset.geo_transform()?;

    let src_spatial_ref = SpatialRef::from_wkt(&dataset.projection())?;
    let dst_spatial_ref = SpatialRef::from_epsg(epsg_code)?;

    let coord_transform = 
        CoordTransform::new(&src_spatial_ref, &dst_spatial_ref)?;

    // compute minimum and maximum x and y coordinates
    let (src_width, src_height) = dataset.size();
    let corner_pixels = vec!((0, 0, 0), (src_width, 0, 0),
        (0, src_height, 0), (src_width, src_height, 0));

    let (xs, ys, _) = coordinate::transform_pixels(&corner_pixels,
        &transform, &coord_transform)?;

    let min_x = xs.iter().cloned().fold(1./0., f64::min);
    let max_x = xs.iter().cloned().fold(0./0., f64::max);
    let min_y = ys.iter().cloned().fold(1./0., f64::min);
    let max_y = ys.iter().cloned().fold(0./0., f64::max);

    println!("IMAGE BOUNDS: {} {} {} {}", min_x, max_x, min_y, max_y);
 
    // open memory driver
    let driver = Driver::get("Mem")?;

    // compute dataset window bounds
    let window_bounds = coordinate::get_window_bounds(min_x, max_x,
        min_y, max_y, x_interval, y_interval);

    let mut results = Vec::new();
    for (win_min_x, win_max_x, win_min_y, win_max_y) in window_bounds.iter() {
        println!("WINDOW BOUNDS: {} {} {} {}", win_min_x, 
            win_max_x, win_min_y, win_max_y);

        // TODO - determine min and max search x and y values
        //  necessary to include 'null' pixels in spatially split image
        let search_min_x = 0 - (src_width as isize / 2);
        let search_max_x = src_width as isize + (src_width as isize / 2);

        let search_min_y = 0 - (src_height as isize/ 2);
        let search_max_y = src_height as isize + (src_height as isize / 2);

        // identify pixels which fall into window bounds
        let mut indices = Vec::new();
        //for i in 0..width {
        for i in search_min_x..search_max_x {
            // check if column contains any valid pixels
            let (lower_col_x, lower_col_y, _) = 
                coordinate::transform_pixel(i, search_min_y,
                    0, &transform, &coord_transform)?;

            let (upper_col_x, upper_col_y, _) = 
                coordinate::transform_pixel(i, search_max_y,
                    0, &transform, &coord_transform)?;

            let col_min_x = lower_col_x.min(upper_col_x);
            let col_max_x = lower_col_x.max(upper_col_x);

            let col_min_y = lower_col_y.min(upper_col_y);
            let col_max_y = lower_col_y.max(upper_col_y);

            if (&col_min_x <= win_max_x && &col_max_x >= win_min_x)
                    && (&col_min_y <= win_max_y && &col_max_y >= win_min_y) {
                // find maximum valid pixel with binary search
                let mut max_index = search_min_y as f64;
                let mut tmp_min_index = search_max_y as f64;
                loop {
                    let mid_index = ((max_index + tmp_min_index) / 2.0).ceil();
                    if mid_index == tmp_min_index {
                        break;
                    }

                    let (_, y, _) = coordinate::transform_pixel(i,
                        mid_index as isize, 0, &transform, &coord_transform)?;

                    if &y <= win_min_y {
                        tmp_min_index = mid_index;
                    } else {
                        max_index = mid_index;
                    }
                }
 
                // find minimum valid pixel with binary search
                let mut tmp_max_index = search_min_y as f64;
                let mut min_index = search_max_y as f64;
                loop {
                    let mid_index = ((tmp_max_index + min_index) / 2.0).ceil();
                   if mid_index == min_index {
                        break;
                    }

                    let (_, y, _) = coordinate::transform_pixel(i,
                        mid_index as isize, 0, &transform, &coord_transform)?;

                    if &y <= win_max_y {
                        min_index = mid_index;
                    } else {
                        tmp_max_index = mid_index;
                    }
                }

                // add column range to indices
                indices.push((i, min_index as isize, max_index as isize));
            }
        }

        // find split image width and height (in pixels)
        let split_min_x = indices[0].0;
        let split_max_x = indices[indices.len() - 1].0;

        let split_min_y = indices.iter()
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap()).unwrap().1;
        let split_max_y = indices.iter()
            .max_by(|a, b| a.2.partial_cmp(&b.2).unwrap()).unwrap().2;

        println!("  PIXEL BOUNDS: {} {} {} {}", split_min_x,
            split_max_x, split_min_y, split_max_y);
        
        // compute raster offsets
        let src_x_offset = split_min_x.max(0) as isize;
        let src_y_offset = split_min_y.max(0) as isize;

        let buf_width = (split_max_x.min(src_width as isize) 
            - split_min_x.max(0)) as usize;
        let buf_height = (split_max_y.min(src_height as isize)
            - split_min_y.max(0)) as usize;

        let dst_x_offset = (0 - split_min_x).max(0) as isize;
        let dst_y_offset = (0 - split_min_y).max(0) as isize;

        let dst_width = (split_max_x - split_min_x) as isize;
        let dst_height = (split_max_y - split_min_y) as isize;

        //println!("  SRC OFFSET: {} {}", src_x_offset, src_y_offset);
        //println!("  BUF DIMENSIONS: {} {}", buf_width, buf_height);

        //println!("  DST OFFSET: {} {}", dst_x_offset, dst_y_offset);
        //println!("  DIMENSIONS: {} {}", dst_width, dst_height);

        // initialize split dataset
        let path = format!("/tmp/{}.{}.{}.{}", win_min_x, 
            win_max_x, win_min_y, win_max_y);
        let split_dataset = driver.create(&path,
            dst_width, dst_height, dataset.count())?;

        // modify transform
        let mut transform = dataset.geo_transform()?;
        transform[0] = transform[0] + (split_min_x as f64 * transform[1])
            + (split_min_y as f64 * transform[2]);
        transform[3] = transform[3] + (split_min_x as f64 * transform[4])
            + (split_min_y as f64 * transform[5]);

        split_dataset.set_geo_transform(&transform)?;
        split_dataset.set_projection(&dataset.projection())?;

        // copy rasterband data to new image
        for i in 0..dataset.count() {
            let rasterband = dataset.rasterband(i + 1)?;

            // read rasterband data into buffer
            let buffer = rasterband.read_as::<u8>((src_x_offset, src_y_offset),
                (buf_width, buf_height), (buf_width, buf_height))?;

            // TODO - remove unecessary pixels
            
            
            // TODO - copy valid pixels to new rasters
            let mut split_data = vec![0u8; (dst_width * dst_height) as usize];
            for (x, start_y, end_y) in indices.iter() {
                if x < &0 || x >= &(src_width as isize) {
                    continue;
                }
 
                // TODO -- iterating over 1 to many x values
                //println!("{} {} {}", x, start_y, end_y);
                let x_pixel = *x as usize - src_x_offset as usize;

                if x_pixel >= buf_width {
                    continue; // TODO - tmp -- no idea if this will work
                }

                for y in *start_y..*end_y {
                    let y = y as usize;
                    if y < 0 || y >= src_height {
                        continue;
                    }

                    let y_pixel = y - src_y_offset as usize;

                    //println!("    copy {} {}", x_pixel, y_pixel);

                    let data_index = (x_pixel * buf_height) + y_pixel;
                    let split_data_index =((x_pixel + dst_x_offset as usize) * dst_height as usize) + y_pixel + dst_y_offset as usize;

                    //println!("      {} {}", data_index, split_data_index);

                    //let split_data_index = ((x - src_x_offset as usize + dst_x_offset as usize) * dst_height as usize) + ((y - src_y_offset as usize + dst_y_offset as usize));

                    //println!(" copy pixel {} / {} to {} / {}", data_index, buffer.data.len(), split_data_index, split_data.len());

                    split_data[split_data_index] = buffer.data[data_index];
                    //println!("copy {} {}", x - src_x_offset as usize, y - src_y_offset as usize);
                }
            }

            // write rasterband data to output dataset
            let raster = Buffer::new((dst_width as usize,
                dst_height as usize), split_data);
            split_dataset.write_raster(i+1, (0, 0),
                (dst_width as usize, dst_height as usize), &raster);
 
            //split_dataset.write_raster(i+1, (dst_x_offset, dst_y_offset),
            //    (buf_width, buf_height), &buffer)?;
        }

        // add output_dataset to return vector
        results.push(split_dataset);

        //break; // TODO - only working on one image
    }

    Ok(results)
}*/

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
