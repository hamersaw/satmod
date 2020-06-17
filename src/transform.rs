use gdal::errors::Error;
use gdal::raster::{Dataset, Driver};
use gdal::spatial_ref::{CoordTransform, SpatialRef};

use crate::prelude::Geocode;

pub struct DatasetSplit<'a> {
    buf_height: usize,
    buf_width: usize,
    dataset: &'a Dataset,
    dst_height: isize,
    dst_width: isize,
    dst_x_offset: isize,
    dst_y_offset: isize,
    min_cx: f64,
    min_px: isize,
    max_cx: f64,
    max_px: isize,
    min_cy: f64,
    min_py: isize,
    max_cy: f64,
    max_py: isize,
    src_x_offset: isize,
    src_y_offset: isize,
}

impl<'a> DatasetSplit<'a> {
    pub fn coordinates(&self) -> (f64, f64, f64, f64) {
        (self.min_cx, self.max_cx, self.min_cy, self.max_cy)
    }

    pub fn dataset(&self) -> Result<Dataset, Error> {
        // open memory driver
        let driver = Driver::get("Mem")?;

        unimplemented!();
        // TODO unimplemented
        /*// initialize split Dataset
        let gdal_type = self.dataset.band_type(1)?;
        let split_dataset = crate::init_dataset(&driver,
            "unreachable", gdal_type, self.dst_width, 
            self.dst_height, self.dataset.count()).unwrap();

        // modify transform
        let mut transform = self.dataset.geo_transform()?;
        transform[0] = transform[0] + (self.min_px as f64 * transform[1])
            + (self.min_py as f64 * transform[2]);
        transform[3] = transform[3] + (self.min_px as f64 * transform[4])
            + (self.min_py as f64 * transform[5]);

        split_dataset.set_geo_transform(&transform)?;
        split_dataset.set_projection(&self.dataset.projection())?;

        // copy rasterband data to new image
        for i in 0..self.dataset.count() {
            crate::copy_raster(self.dataset, i+1, 
                (self.src_x_offset, self.src_y_offset),
                (self.buf_width, self.buf_height),
                &split_dataset, i+1,
                (self.dst_x_offset, self.dst_y_offset), 
                (self.buf_width, self.buf_height))?;
        }

        Ok(split_dataset)*/
    }

    pub fn pixels(&self) -> (isize, isize, isize, isize) {
        (self.min_px, self.max_px, self.min_py, self.max_py)
    }
}

pub fn split(dataset: &Dataset, geocode: Geocode,
        precision: usize) -> Result<Vec<DatasetSplit>, Error> {
    // initialize transform array and CoordTransform's from dataset
    let transform = dataset.geo_transform()?;

    let src_spatial_ref = SpatialRef::from_wkt(&dataset.projection())?;
    let dst_spatial_ref = SpatialRef::from_epsg(
        geocode.get_epsg_code())?;

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

    let (xs, ys, _) = crate::coordinate::transform_pixels(
        &corner_pixels, &transform, &coord_transform)?;

    let image_min_cx = xs.iter().cloned().fold(1./0., f64::min);
    let image_max_cx = xs.iter().cloned().fold(0./0., f64::max);
    let image_min_cy = ys.iter().cloned().fold(1./0., f64::min);
    let image_max_cy = ys.iter().cloned().fold(0./0., f64::max);

    //println!("IMAGE BOUNDS: {} {} {} {}",
    //    image_min_cx, image_max_cx, image_min_cy, image_max_cy);

    // compute dataset window bounds
    let (x_interval, y_interval) = geocode.get_intervals(precision);
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

        // add DatasetSplit
        results.push(DatasetSplit {
            buf_height: buf_height,
            buf_width: buf_width,
            dataset: dataset,
            dst_height: dst_height,
            dst_width: dst_width,
            dst_x_offset: dst_x_offset,
            dst_y_offset: dst_y_offset,
            min_cx: *win_min_cx,
            min_px: bound_min_px,
            max_cx: *win_max_cx,
            max_px: bound_max_px,
            min_cy: *win_min_cy,
            min_py: bound_min_py,
            max_cy: *win_max_cy,
            max_py: bound_max_py,
            src_x_offset: src_x_offset,
            src_y_offset: src_y_offset,
        });
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use gdal::raster::{Dataset, Driver};
    use gdal_sys::GDALDataType;

    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::path::Path;

    /*#[test]
    fn image_split() {
        let path = Path::new("examples/L1C_T13TDE_A003313_20171024T175403");
        //let path = Path::new("examples/T13TDF_20150821T180236_B01.jp2");

        // read dataset
        let dataset = Dataset::open(path).expect("dataset open");

        // open gtiff driver
        let driver = Driver::get("GTiff").expect("get driver");

        // iterate over geohash split datasets
        let (y_interval, x_interval) =
            crate::coordinate::get_geohash_intervals(4);
        let mut count = 0;
        for dataset_split in super::split(&dataset, 4326,
                x_interval, y_interval).expect("split dataset") {
            println!("IMAGE: {}", count);
            count += 1;

            let dataset = dataset_split.dataset()
                .expect("perform split operation");
            let pixel_coverage = crate::coverage(&dataset)
                .expect("dataset pixel coverage");
            println!("  {}", pixel_coverage);

            // copy memory datasets to gtiff files
            //dataset.create_copy(&driver,
            //    &format!("/tmp/st-image-{}.tif", count), None)
            //    .expect("dataset copy");
        }
    }*/
}
