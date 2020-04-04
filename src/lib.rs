use gdal::errors::Error;
use gdal::raster::{Dataset, Driver};
use gdal::spatial_ref::{CoordTransform, SpatialRef};
use geohash::{self, Coordinate};

mod spatial;

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

pub fn split(dataset: &Dataset, precision: usize)
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
    let bounds = spatial::get_coordinate_bounds(lat_min,
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
        let output_dataset = driver.create(&path, dst_width,
            dst_height, dataset.count(), None)?;

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
}

#[cfg(test)]
mod tests {
    use gdal::raster::{Dataset, Driver};
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

            println!("{} {:?}", geohash, super::coverage(&dataset));

            // copy memory datasets to gtiff files
            dataset.create_copy(&driver, &format!("/tmp/{}", geohash))
                .expect("dataset copy");
        }
    }
}
