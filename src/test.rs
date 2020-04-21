use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use gdal::errors::Error;
use gdal::raster::{Buffer, Dataset, Driver};
use gdal::spatial_ref::{CoordTransform, SpatialRef};
use geohash::{self, Coordinate};

pub fn split(dataset: &Dataset, epsg_code: u32, 
        x_interval: f64, y_interval: f64) 
        -> Result<Vec<(Dataset, f64, f64, f64, f64)>, Error> {
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

    let (xs, ys, _) = crate::coordinate::transform_pixels(&corner_pixels,
        &transform, &coord_transform)?;

    let min_x = xs.iter().cloned().fold(1./0., f64::min);
    let max_x = xs.iter().cloned().fold(0./0., f64::max);
    let min_y = ys.iter().cloned().fold(1./0., f64::min);
    let max_y = ys.iter().cloned().fold(0./0., f64::max);

    println!("IMAGE BOUNDS: {} {} {} {}", min_x, max_x, min_y, max_y);
 
    // open memory driver
    let driver = Driver::get("Mem")?;

    // compute dataset window bounds
    let window_bounds = crate::coordinate::get_window_bounds(min_x, max_x,
        min_y, max_y, x_interval, y_interval);

    let mut results = Vec::new();
    for (win_min_x, win_max_x, win_min_y, win_max_y) in window_bounds.iter() {
        println!("WINDOW BOUNDS: {} {} {} {}", win_min_x, 
            win_max_x, win_min_y, win_max_y);

        // TODO - determine min and max search x and y values
        //  necessary to include 'null' pixels in spatially split image
        let search_min_x = 0 - (src_width as isize / 2);
        let search_max_x = src_width as isize + (src_width as isize / 2);

        let search_min_y = 0 - (src_height as isize / 2);
        let search_max_y = src_height as isize + (src_height as isize / 2);

        // identify pixels which fall into window bounds
        let mut indices = Vec::new();
        for i in search_min_x..search_max_x {
            // check if column contains any valid pixels
            let (lower_col_x, lower_col_y, _) = 
                crate::coordinate::transform_pixel(i, search_min_y,
                    0, &transform, &coord_transform)?;

            let (upper_col_x, upper_col_y, _) = 
                crate::coordinate::transform_pixel(i, search_max_y,
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

                    let (_, y, _) = crate::coordinate::transform_pixel(i,
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

                    let (_, y, _) = crate::coordinate::transform_pixel(i,
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
            /*let mut split_data = vec![0u8; (dst_width * dst_height) as usize];
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
                    let split_data_index = ((x_pixel + dst_x_offset as usize) * dst_height as usize) + y_pixel + dst_y_offset as usize;

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
                (dst_width as usize, dst_height as usize), &raster);*/
 
            split_dataset.write_raster(i+1, (dst_x_offset, dst_y_offset),
                (buf_width, buf_height), &buffer)?;
        }

        // add output_dataset to return vector
        results.push((split_dataset, *win_min_x,
            *win_max_x, *win_min_y, *win_max_y));

        //break; // TODO - only working on one image
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use gdal::raster::{Dataset, Driver};
    use gdal::spatial_ref::{CoordTransform, SpatialRef};
    use geohash::{self, Coordinate};

    use std::collections::HashMap;
    use std::path::{Path, PathBuf};

    #[test]
    fn image_split() {
        let geohash_precision = 4;
        let filename = "L1C_T13TDE_A003313_20171024T175403";
        let filepath = format!("examples/{}", filename);

        let read_path = Path::new(&filepath);

        // read dataset
        let dataset = Dataset::open(read_path).expect("dataset open");

        // open gtiff driver
        let driver = Driver::get("GTiff").expect("get driver");

        // iterate over geohash split datasets
        let (y_interval, x_interval) =
            crate::coordinate::get_geohash_intervals(geohash_precision);
        let mut write_path = PathBuf::from("/tmp/st-image/");
        for (dataset, min_x, max_x, min_y, max_y) in 
                super::split(&dataset, 4326, x_interval, y_interval)
                    .expect("split dataset").iter() {

        /*for (i, dataset) in super::split(&dataset, 3857, 40000.0,
                40000.0).expect("split dataset").iter().enumerate() {*/

        /*for (i, dataset) in super::split(&dataset, 32613, 40000.0,
                40000.0).expect("split dataset").iter().enumerate() {*/

            // compute image geohash
            let coordinate = Coordinate { x: *max_x, y: *max_y };    
            let geohash = geohash::encode(coordinate, geohash_precision)
                .expect("geohash");

            // copy memory datasets to gtiff files
            write_path.push(&geohash);
            std::fs::create_dir_all(&write_path);

            write_path.push(filename);
            //dataset.create_copy(&driver, &write_path.to_string_lossy())
            //    .expect("dataset copy");
 
            // read dataset raster data
            let mut rasters = Vec::new();
            for i in 0..dataset.count() {
                let raster=
                    dataset.read_full_raster_as::<u8>(i + 1).unwrap();
                rasters.push(raster);
            }

            // initialize transform array and CoordTransform's from dataset
            let transform = dataset.geo_transform().expect("transform");

            let src_spatial_ref = 
                SpatialRef::from_wkt(&dataset.projection()).expect("src projection");
            let dst_spatial_ref = SpatialRef::from_epsg(4326).expect("dst_projection");

            let coord_transform = CoordTransform::new(&src_spatial_ref,
                &dst_spatial_ref).expect("coordinate transform");

            // compute minimum and maximum x and y coordinates
            let (src_width, src_height) = dataset.size();
            let mut pixels = Vec::new();
            for i in 0..src_height {
                for j in 0..src_width {
                    pixels.push((j, i, 0));
                }
            }

            let (xs, ys, _) = crate::coordinate::transform_pixels(
                &pixels, &transform, &coord_transform).expect("transform pixels");

            for i in 0..xs.len() {
                let coordinate = Coordinate{ x: xs[i], y: ys[i] };    
                let pixel_geohash = geohash::encode(coordinate, geohash_precision)
                    .expect("geohash");

                if pixel_geohash != geohash {
                    // set pixel red 
                    rasters[0].data[i] = 255;
                    rasters[1].data[i] = 0;
                    rasters[2].data[i] = 0;
                } else {
                    // set pixel green
                    rasters[0].data[i] = 0;
                    rasters[1].data[i] = 255;
                    rasters[2].data[i] = 0;
                }
            }

            // TODO - write out dataset
            // initialize dataset - TODO error
            println!("writing: {:?}", write_path);
            let driver = Driver::get("GTiff").unwrap();
            let mask_dataset = driver.create(&write_path.to_string_lossy(),
                src_width as isize, src_height as isize, rasters.len() as isize).unwrap();

            mask_dataset.set_geo_transform(&transform).unwrap();
            mask_dataset.set_projection(&dataset.projection()).unwrap();
         
            // read rasterbands
            for i in 0..rasters.len() {
                // write raster to dataset - TODO error
                mask_dataset.write_raster(i as isize + 1, (0, 0),
                    (src_width as usize, src_height as usize),
                    &rasters[i]).unwrap();
            }

            // remove filename and geohash path elements
            write_path.pop();
            write_path.pop();
        }
    }
}
