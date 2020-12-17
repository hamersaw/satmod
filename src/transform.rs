use gdal::{Dataset, Driver};
use gdal::spatial_ref::CoordTransform;

use std::error::Error;

pub fn merge(datasets: &[Dataset])
        -> Result<Dataset, Box<dyn Error>> {
    // TODO - ensure datasets are in same spatial reference system

    // find minimum and maximum coordinates
    let mut min_cx = f64::MAX;
    let mut max_cx = f64::MIN;
    let mut min_cy = f64::MAX;
    let mut max_cy = f64::MIN;

    for dataset in datasets.iter() {
        // TODO ensure transforms match

        let transform = dataset.geo_transform()?;
        let (src_width, src_height) = dataset.raster_size();
        let (width, height) = (src_width as f64, src_height as f64);

        let image_min_cx = transform[0];
        let image_max_cx = transform[0] + (width * transform[1])
            + (height * transform[2]);
        let image_min_cy = transform[3] + (width * transform[4])
            + (height * transform[5]);
        let image_max_cy = transform[3];

        min_cx = min_cx.min(image_min_cx);
        max_cx = max_cx.max(image_max_cx);
        min_cy = min_cy.min(image_min_cy);
        max_cy = max_cy.max(image_max_cy);
    }

    //println!("DST IMAGE BOUNDS {} {} {} {}",
    //    min_cx, max_cx, min_cy, max_cy);

    // compute merged image dimensions
    let transform = datasets[0].geo_transform()?;
    let min_px = (min_cx - transform[0]) / transform[1];
    let max_px = (max_cx - transform[0]) / transform[1];
    let min_py = (min_cy - transform[3]) / transform[5] * -1.0;
    let max_py = (max_cy - transform[3]) / transform[5] * -1.0;

    //println!("  PIXELS {} {} {} {}", min_px, max_px, min_py, max_py);

    let dst_width = (max_px - min_px) as isize;
    let dst_height = (max_py - min_py) as isize;
    //println!("DST IMAGE DIMENSIONS {} {}", dst_width, dst_height);

    // open memory driver
    let driver = Driver::get("Mem")?;

    // initialize merge Dataset
    let rasterband = datasets[0].rasterband(1)?;
    let gdal_type = rasterband.band_type();
    let no_data_value = rasterband.no_data_value();

    let merge_dataset = crate::init_dataset(&driver,
        "unreachable", gdal_type, dst_width, dst_height,
        datasets[0].raster_count(), no_data_value)?;

    // modify transform
    let mut merge_transform = datasets[0].geo_transform()?;
    merge_transform[0] = min_cx;
    merge_transform[3] = max_cy;

    merge_dataset.set_geo_transform(&merge_transform)?;
    merge_dataset.set_projection(&datasets[0].projection())?;

    // copy source rasters
    for dataset in datasets.iter() {
        // compute raster offsets
        let transform = dataset.geo_transform()?;
        let (src_width, src_height) = dataset.raster_size();

        let dst_x_offset = ((transform[0] - merge_transform[0])
            / merge_transform[1]) as isize;
        let dst_y_offset = ((transform[3] - merge_transform[3])
            / merge_transform[5]) as isize;

        // copy all rasters
        for i in 0..dataset.raster_count() {
            crate::copy_raster(dataset, i+1, 
                (0, 0),
                (src_width, src_height),
                &merge_dataset, i+1,
                (dst_x_offset, dst_y_offset), 
                (src_width, src_height))?;
        }
    }
    
    Ok(merge_dataset)
}

pub fn split(dataset: &Dataset, min_cx: f64, max_cx: f64,
        min_cy : f64, max_cy: f64, epsg_code: u32)
        -> Result<Option<Dataset>, Box<dyn Error>> {
    let (src_width, src_height) = dataset.raster_size();

    // initialize CoordTransforms from dataset
    let (mut transform, projection, src_spatial_ref, dst_spatial_ref) =
        crate::coordinate::get_transform_refs(dataset, epsg_code)?;
    let coord_transform = CoordTransform::new(
        &src_spatial_ref, &dst_spatial_ref)?;
    let reverse_transform = CoordTransform::new(
        &dst_spatial_ref, &src_spatial_ref)?;

    // compute center point pixels
    let mid_cx = (min_cx + max_cx) / 2.0;
    let mid_cy = (min_cy + max_cy) / 2.0;

    let (center_tx, center_ty, _) = crate::coordinate::transform_coord(
        mid_cx, mid_cy, 0.0, &reverse_transform)?;

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
        if bound_min_cx <= min_cx
                && bound_max_cx >= max_cx
                && bound_min_cy <= min_cy
                && bound_max_cy >= max_cy {
                //&& &bound_max_cy >= min_cy { TODO - validate fix?
            break;
        }

        // increment one of the bounds
        // TODO - need to fix this in the case where y transforms are non-negative
        let bound_differences = vec![
            bound_min_cx - min_cx,
            max_cx - bound_max_cx, 
            bound_min_cy - min_cy,
            max_cy - bound_max_cy
        ];

        let (mut index, mut value) = (0, bound_differences[0]);
        for (i, x) in bound_differences.iter().enumerate().skip(1) {
            if x > &value {
                value = *x;
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
        return Ok(None);
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

    // open memory driver
    let driver = Driver::get("Mem")?;

    // initialize split Dataset
    let rasterband = dataset.rasterband(1)?;
    let gdal_type = rasterband.band_type();
    let no_data_value = rasterband.no_data_value();

    let split_dataset = crate::init_dataset(&driver,
        "unreachable", gdal_type, dst_width, dst_height,
        dataset.raster_count(), no_data_value)?;

    // modify transform
    //let mut transform = dataset.geo_transform()?;
    transform[0] = transform[0] + (bound_min_px as f64 * transform[1])
        + (bound_min_py as f64 * transform[2]);
    transform[3] = transform[3] + (bound_min_px as f64 * transform[4])
        + (bound_min_py as f64 * transform[5]);

    split_dataset.set_geo_transform(&transform)?;
    split_dataset.set_projection(&projection)?;

    // copy rasterband data to new image
    for i in 0..dataset.raster_count() {
        crate::copy_raster(dataset, i+1, 
            (src_x_offset, src_y_offset),
            (buf_width, buf_height),
            &split_dataset, i+1,
            (dst_x_offset, dst_y_offset), 
            (buf_width, buf_height))?;
    }

    Ok(Some(split_dataset))
}

#[cfg(test)]
mod tests {
    //use crate::coordinate::Geocode;

    //use gdal::{Dataset, Driver};
    //use gdal_sys::GDALDataType;

    //use std::path::Path;

    /*#[test]
    fn transform_merge() {
        // read in datasets
        let mut datasets = Vec::new();
        for entry in std::fs::read_dir("examples/split")
                .expect("read dir") {
            let entry = entry.expect("parse entry");

            let dataset = Dataset::open(&entry.path())
                .expect("dataset open");
            datasets.push(dataset);
        }

        // merge datasets
        let dataset = crate::transform::merge(&datasets).expect("merge");

        // open gtiff driver
        //let driver = Driver::get("GTiff").expect("get driver");
        //dataset.create_copy(&driver, "/tmp/merge.tif")
        //    .expect("dataset copy");
    }*/

    /*#[test]
    fn transform_split_geohash4() {
        // read dataset
        let path = Path::new("examples/full/L1C_T13TDE_A003313_20171024T175403");
        let dataset = Dataset::open(path).expect("dataset open");

        // compute geohash window boundaries for dataset
        let geocode = Geocode::Geohash;
        let epsg_code = geocode.get_epsg_code();
        let (x_interval, y_interval) = geocode.get_intervals(4);

        let (image_min_cx, image_max_cx, image_min_cy, image_max_cy) =
            crate::coordinate::get_bounds(&dataset, epsg_code)
                .expect("get bounds");

        let window_bounds = crate::coordinate::get_windows(
            image_min_cx, image_max_cx, image_min_cy, image_max_cy,
                x_interval, y_interval);

        // open gtiff driver
        let driver = Driver::get("GTiff").expect("get driver");

        // split dataset along geohash boundaries
        let mut count = 0;
        for (min_cx, max_cx, min_cy, max_cy) in window_bounds {
            let split_dataset = match crate::transform::split(&dataset,
                    min_cx, max_cx, min_cy, max_cy, epsg_code) {
                Ok(split_dataset) => split_dataset,
                Err(_) => continue,
            };

            // copy memory datasets to gtiff files
            //split_dataset.create_copy(&driver,
            //    &format!("examples/split/st-image-{}.tif", count))
            //    .expect("dataset copy");

            // compute pixel coverage over split image
            //let pixel_coverage = crate::coverage(&dataset)
            //    .expect("dataset pixel coverage");
            //println!("{} - {}", count, pixel_coverage);

            count += 1;
        }
    }*/
}
