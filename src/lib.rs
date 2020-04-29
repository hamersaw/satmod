use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use gdal::errors::Error;
use gdal::raster::{Buffer, Dataset, Driver};
use gdal::spatial_ref::{CoordTransform, SpatialRef};
use gdal_sys::GDALDataType;

use std::io::{Read, Write};

pub mod coordinate;

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

pub fn split(dataset: &Dataset, epsg_code: u32, 
        x_interval: f64, y_interval: f64) 
        -> Result<Vec<(Dataset, f64, f64, f64, f64)>, Error> {
    // initialize transform array and CoordTransform's from dataset
    let transform = dataset.geo_transform()?;

    let src_spatial_ref = SpatialRef::from_wkt(&dataset.projection())?;
    let dst_spatial_ref = SpatialRef::from_epsg(epsg_code)?;

    let coord_transform = 
        CoordTransform::new(&src_spatial_ref, &dst_spatial_ref)?;
    let reverse_transform =
        CoordTransform::new(&dst_spatial_ref, &src_spatial_ref)?;

    // compute minimum and maximum x and y coordinates
    let (src_width, src_height) = dataset.size();
    let corner_pixels = vec![
        (0, 0, 0),
        (src_width as isize, 0, 0),
        (0, src_height as isize, 0),
        (src_width as isize, src_height as isize, 0)
    ];

    let (xs, ys, _) = crate::coordinate::transform_pixels(&corner_pixels,
        &transform, &coord_transform)?;

    let image_min_cx = xs.iter().cloned().fold(1./0., f64::min);
    let image_max_cx = xs.iter().cloned().fold(0./0., f64::max);
    let image_min_cy = ys.iter().cloned().fold(1./0., f64::min);
    let image_max_cy = ys.iter().cloned().fold(0./0., f64::max);

    //println!("IMAGE BOUNDS: {} {} {} {}",
    //    image_min_cx, image_max_cx, image_min_cy, image_max_cy);
 
    // open memory driver
    let driver = Driver::get("Mem")?;

    // compute dataset window bounds
    let window_bounds = crate::coordinate::get_window_bounds(
        image_min_cx, image_max_cx, image_min_cy,
        image_max_cy, x_interval, y_interval);

    let mut results = Vec::new();
    for (win_min_cx, win_max_cx, win_min_cy, win_max_cy)
            in window_bounds.iter() {
        //println!("WINDOW BOUNDS: {} {} {} {}", win_min_cx, 
        //    win_max_cx, win_min_cy, win_max_cy);

        // compute center point pixels
        let win_mid_cx = (win_min_cx + win_max_cx) / 2.0;
        let win_mid_cy = (win_min_cy + win_max_cy) / 2.0;

        let (center_tx, center_ty, _) =
            crate::coordinate::transform_coord(win_mid_cx,
                win_mid_cy, 0.0, &reverse_transform)?;

        let center_px = (center_tx - transform[0]) / transform[1];
        let center_py = (center_ty - transform[3]) / transform[5];

        // compute window pixel bounding box
        let mut bound_min_px = center_px as isize;
        let mut bound_max_px = center_px as isize;
        let mut bound_min_py = center_py as isize;
        let mut bound_max_py = center_py as isize;

        let mut bound_min_cx;
        let mut bound_max_cx;
        let mut bound_min_cy;
        let mut bound_max_cy;

        loop {
            // convert bounding pixels to coordinates
            let pixels = vec![
                (bound_min_px, bound_min_py, 0),
                (bound_max_px, bound_min_py, 0),
                (bound_min_px, bound_max_py, 0),
                (bound_max_px, bound_max_py, 0)
            ];

            let (xs, ys, _) = crate::coordinate::transform_pixels(
                &pixels, &transform, &coord_transform)?;

            bound_min_cx = xs[0].max(xs[2]);
            bound_max_cx = xs[1].min(xs[3]);
            bound_min_cy = ys[2].max(ys[3]);
            bound_max_cy = ys[0].min(ys[1]);

            // check if bounding box envolopes window
            if &bound_min_cx <= win_min_cx
                    && &bound_max_cx >= win_max_cx
                    && &bound_min_cy <= win_min_cy
                    && &bound_max_cy >= win_min_cy {
                break;
            }
 
            // increment one of the bounds
            // TODO - need to fix this in the case where transforms are non-negative
            let bound_differences = vec![
                bound_min_cx - *win_min_cx,
                *win_max_cx - bound_max_cx, 
                bound_min_cy - *win_min_cy,
                *win_max_cy - bound_max_cy
            ];

            let (mut index, mut value) = (0, bound_differences[0]);
            for i in 1..bound_differences.len() {
                if bound_differences[i] > value {
                    value = bound_differences[i];
                    index = i;
                }
            }

            match index {
                0 => bound_min_px -= 1,
                1 => bound_max_px += 1,
                2 => bound_max_py += 1,
                3 => bound_min_py -= 1,
                _ => unreachable!(),
            }
        }

        //println!("  PIXEL BOUNDS: {} {} {} {}", bound_min_px, 
        //    bound_max_px, bound_min_py, bound_max_py);

        //println!("  COORDINATE BOUNDS: {} {} {} {}", bound_min_cx, 
        //    bound_max_cx, bound_min_cy, bound_max_cy);

        //println!("  IMAGE DIMENSIONS: {} {}", 
        //    bound_max_px - bound_min_px, bound_max_py - bound_min_py);

        // skip window if the pixel boundaries don't fall within image
        if bound_max_px < 0 || bound_min_px >= src_width as isize
                || bound_max_py < 0 || bound_min_py >= src_height as isize {
            continue;
        }

        // compute raster offsets
        let src_x_offset = bound_min_px.max(0) as isize;
        let src_y_offset = bound_min_py.max(0) as isize;

        let buf_width = (bound_max_px.min(src_width as isize) 
            - bound_min_px.max(0)) as usize;
        let buf_height = (bound_max_py.min(src_height as isize)
            - bound_min_py.max(0)) as usize;

        let dst_x_offset = (0 - bound_min_px).max(0) as isize;
        let dst_y_offset = (0 - bound_min_py).max(0) as isize;

        let dst_width = (bound_max_px - bound_min_px) as isize;
        let dst_height = (bound_max_py - bound_min_py) as isize;

        //println!("  SRC OFFSET: {} {}", src_x_offset, src_y_offset);
        //println!("  SRC DIMENSIONS: {} {}", buf_width, buf_height);

        //println!("  DST OFFSET: {} {}", dst_x_offset, dst_y_offset);
        //println!("  DST DIMENSIONS: {} {}", dst_width, dst_height);

        // initialize split dataset
        let path = format!("unreachable");
        let split_dataset = match dataset.band_type(1) {
            Ok(GDALDataType::GDT_Byte) =>
                driver.create_with_band_type::<u8>(&path,
                    dst_width, dst_height, dataset.count())?,
            Ok(GDALDataType::GDT_UInt16) =>
                driver.create_with_band_type::<u16>(&path,
                    dst_width, dst_height, dataset.count())?,
            Ok(_) => unimplemented!(),
            Err(e) => return Err(e),
        };
        /*let gdal_type = dataset.band_type(1)
            .unwrap_or(GDALDataType::GDT_Byte);

        let split_dataset = driver.create_with_band_type::<gdal_type>(
            &path, dst_width, dst_height, dataset.count())?;*/

        // modify transform
        let mut transform = dataset.geo_transform()?;
        transform[0] = transform[0] + (bound_min_px as f64 * transform[1])
            + (bound_min_py as f64 * transform[2]);
        transform[3] = transform[3] + (bound_min_px as f64 * transform[4])
            + (bound_min_py as f64 * transform[5]);

        split_dataset.set_geo_transform(&transform)?;
        split_dataset.set_projection(&dataset.projection())?;

        // copy rasterband data to new image
        for i in 0..dataset.count() {
            let rasterband = dataset.rasterband(i + 1)?;
            let band_type = rasterband.band_type();

            match band_type {
                GDALDataType::GDT_Byte => {
                    // read rasterband data into buffer
                    let buffer = rasterband.read_as::<u8>(
                        (src_x_offset, src_y_offset),
                        (buf_width, buf_height), 
                        (buf_width, buf_height))?;
         
                    split_dataset.write_raster::<u8>(i+1, 
                        (dst_x_offset, dst_y_offset),
                        (buf_width, buf_height), &buffer)?;
                },
                GDALDataType::GDT_UInt16 => {
                    // read rasterband data into buffer
                    let buffer = rasterband.read_as::<u16>(
                        (src_x_offset, src_y_offset),
                        (buf_width, buf_height), 
                        (buf_width, buf_height))?;
         
                    split_dataset.write_raster::<u16>(i+1, 
                        (dst_x_offset, dst_y_offset),
                        (buf_width, buf_height), &buffer)?;
                },
                _ => unimplemented!(),
            }
        }

        // add output_dataset to return vector
        results.push((split_dataset, *win_min_cx,
            *win_max_cx, *win_min_cy, *win_max_cy));
    }

    Ok(results)
}

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
    use gdal::raster::{Dataset, Driver};
    use gdal_sys::GDALDataType;

    use std::collections::BTreeMap;
    //use std::io::Cursor;
    use std::path::Path;

    #[test]
    fn image_split() {
        //let path = Path::new("examples/L1C_T13TDE_A003313_20171024T175403");
        let path = Path::new("examples/T13TDF_20150821T180236_B01.jp2");

        // read dataset
        let dataset = Dataset::open(path).expect("dataset open");

        // open gtiff driver
        let driver = Driver::get("GTiff").expect("get driver");

        // iterate over geohash split datasets
        let (y_interval, x_interval) =
            super::coordinate::get_geohash_intervals(4);
        let mut count = 0;
        for (dataset, _, max_x, _, max_y) in super::split(&dataset, 4326,
                x_interval, y_interval) .expect("split dataset") {
            if super::coverage(&dataset).unwrap_or(0.0) == 0.0 {
                continue;
            }

            // count pixel values in band
            println!("IMAGE: {}", count);

            // iterate over rasterbands
            for i in 0..dataset.count() {
                let rasterband = dataset.rasterband(i + 1)
                    .expect("retrieve rasterband");

                // read rasterband data into buffer
                let band_type = rasterband.band_type();

                match band_type {
                    GDALDataType::GDT_Byte => {
                        /*let buffer = rasterband.read_band_as::<u8>()
                            .expect("reading raster");

                        // iterate over pixels
                        let mut map = BTreeMap::new();
                        for pixel in buffer.data.iter() {
                            let count = map.entry(pixel / 10)
                                .or_insert(0);
                            *count += 1;
                        }

                        for (pixel, count) in map.iter() {
                            println!("  {} : {}", pixel * 10, count);
                        }*/
                    },
                    GDALDataType::GDT_UInt16 => {
                        /*let buffer = rasterband.read_band_as::<u16>()
                            .expect("reading raster");

                        // iterate over pixels
                        let mut map = BTreeMap::new();
                        for pixel in buffer.data.iter() {
                            let count = map.entry(pixel / 1000)
                                .or_insert(0);
                            *count += 1;
                        }

                        for (pixel, count) in map.iter() {
                            println!("  {} : {}", pixel * 1000, count);
                        }*/
                    },
                    _ => unimplemented!(),
                }
            }

            // copy memory datasets to gtiff files
            dataset.create_copy(&driver,
                &format!("/tmp/st-image-{}.tif", count), None)
                .expect("dataset copy");
            count += 1;
        }
    }

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
