use failure::ResultExt;
use gdal::raster::{Dataset, Driver};
use gdal::spatial_ref::{CoordTransform, SpatialRef};

use std::error::Error;

pub fn merge(_datasets: &Vec<Dataset>)
        -> Result<Dataset, Box<dyn Error>> {
    // TODO - implement
    unimplemented!();
}

pub fn split(dataset: &Dataset, min_cx: f64, max_cx: f64, min_cy : f64,
        max_cy: f64, epsg_code: u32) -> Result<Dataset, Box<dyn Error>> {
    // initialize transform array and CoordTransform's from dataset
    let transform = dataset.geo_transform().compat()?;

    let src_spatial_ref = SpatialRef::from_wkt(
        &dataset.projection()).compat()?;
    let dst_spatial_ref = SpatialRef::from_epsg(epsg_code).compat()?;

    let coord_transform = CoordTransform::new(
        &src_spatial_ref, &dst_spatial_ref).compat()?;
    let reverse_transform = CoordTransform::new(
        &dst_spatial_ref, &src_spatial_ref).compat()?;

    let (src_width, src_height) = dataset.size();

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
        // TODO - need to fix this in the case where transforms are non-negative
        let bound_differences = vec![
            bound_min_cx - min_cx,
            max_cx - bound_max_cx, 
            bound_min_cy - min_cy,
            max_cy - bound_max_cy
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
        return Err("pixel boundaries do not fall within image".into());
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
    let driver = Driver::get("Mem").compat()?;

    // initialize split Dataset
    let rasterband = dataset.rasterband(1).compat()?;
    let gdal_type = rasterband.band_type();
    let no_data_value = rasterband.no_data_value();

    let split_dataset = crate::init_dataset(&driver,
        "unreachable", gdal_type, dst_width, dst_height,
        dataset.count(), no_data_value)?;

    // modify transform
    let mut transform = dataset.geo_transform().compat()?;
    transform[0] = transform[0] + (bound_min_px as f64 * transform[1])
        + (bound_min_py as f64 * transform[2]);
    transform[3] = transform[3] + (bound_min_px as f64 * transform[4])
        + (bound_min_py as f64 * transform[5]);

    split_dataset.set_geo_transform(&transform).compat()?;
    split_dataset.set_projection(&dataset.projection()).compat()?;

    // copy rasterband data to new image
    for i in 0..dataset.count() {
        crate::copy_raster(dataset, i+1, 
            (src_x_offset, src_y_offset),
            (buf_width, buf_height),
            &split_dataset, i+1,
            (dst_x_offset, dst_y_offset), 
            (buf_width, buf_height))?;
    }

    Ok(split_dataset)
}

#[cfg(test)]
mod tests {
    use crate::coordinate::Geocode;

    use gdal::raster::{Dataset, Driver};
    use gdal_sys::GDALDataType;

    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::path::Path;

    #[test]
    fn image_geohash_split() {
        // read dataset
        let path = Path::new("examples/L1C_T13TDE_A003313_20171024T175403");
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
                Err(e) => continue,
            };

            // copy memory datasets to gtiff files
            //split_dataset.create_copy(&driver,
            //    &format!("/tmp/st-image-{}.tif", count))
            //    .expect("dataset copy");

            // compute pixel coverage over split image
            //let pixel_coverage = crate::coverage(&dataset)
            //    .expect("dataset pixel coverage");
            //println!("{} - {}", count, pixel_coverage);

            count += 1;
        }
    }
}
