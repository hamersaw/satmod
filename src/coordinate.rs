use gdal::Dataset;
use gdal::spatial_ref::{CoordTransform, SpatialRef};

use std::error::Error;

pub type WindowBounds = (Vec<f64>, Vec<f64>, Vec<f64>);

pub fn get_bounds(dataset: &Dataset, epsg_code: u32)
        -> Result<(f64, f64, f64, f64), Box<dyn Error>> {
    // initialize transform array and CoordTransform's from dataset
    let transform = dataset.geo_transform()?;

    let src_spatial_ref = SpatialRef::from_wkt(
        &dataset.projection())?;
    let dst_spatial_ref = SpatialRef::from_epsg(epsg_code)?;

    #[cfg(major_ge_3)]
    {
        use gdal_sys::OSRAxisMappingStrategy;
        src_spatial_ref.set_axis_mapping_strategy(
            OSRAxisMappingStrategy::OAMS_TRADITIONAL_GIS_ORDER);
        dst_spatial_ref.set_axis_mapping_strategy(
            OSRAxisMappingStrategy::OAMS_TRADITIONAL_GIS_ORDER);
    }

    let coord_transform = CoordTransform::new(
        &src_spatial_ref, &dst_spatial_ref)?;

    // compute minimum and maximum x and y coordinates
    let (src_width, src_height) = dataset.raster_size();
    let corner_pixels = vec![
        (0, 0, 0),
        (src_width as isize, 0, 0),
        (0, src_height as isize, 0),
        (src_width as isize, src_height as isize, 0)
    ];

    let (xs, ys, _) = transform_pixels(&corner_pixels,
        &transform, &coord_transform)?;

    let min_cx = xs.iter().cloned().fold(1./0., f64::min);
    let max_cx = xs.iter().cloned().fold(f64::NAN, f64::max);
    let min_cy = ys.iter().cloned().fold(1./0., f64::min);
    let max_cy = ys.iter().cloned().fold(f64::NAN, f64::max);

    Ok((min_cx, max_cx, min_cy, max_cy))
}

pub fn get_windows(min_x: f64, max_x: f64, min_y: f64, max_y: f64,
        x_interval: f64, y_interval: f64) -> Vec<(f64, f64, f64, f64)> {
    // compute indices for minimum and maximum coordinates
    let min_x_index = (min_x / x_interval).floor() as i32;
    let max_x_index = (max_x / x_interval).ceil() as i32;

    let min_y_index = (min_y / y_interval).floor() as i32;
    let max_y_index = (max_y / y_interval).ceil() as i32;

    // compute all window bounds
    let mut window_bounds = Vec::new();
    for x_index in min_x_index..max_x_index {
        let x_index = x_index as f64;

        for y_index in min_y_index..max_y_index {
            let y_index = y_index as f64;

            // compute window x and y bounds
            let window_x_min = x_index * x_interval;
            let window_x_max = (x_index + 1.0) * x_interval;

            let window_y_min = y_index * y_interval;
            let window_y_max = (y_index + 1.0) * y_interval;

            // add to window bounds
            window_bounds.push((window_x_min,
                window_x_max, window_y_min, window_y_max));
        }
    }

    window_bounds
}

pub fn transform_pixel(x: isize, y: isize, z: isize,
        transform: &[f64; 6], coord_transform: &CoordTransform)
        -> Result<(f64, f64, f64), Box<dyn Error>> {
    let x_coord = transform[0] + (x as f64 * transform[1])
        + (y as f64 * transform[2]);
    let y_coord = transform[3] + (x as f64 * transform[4])
        + (y as f64 * transform[5]);

    transform_coord(x_coord, y_coord, z as f64, coord_transform)
}

pub fn transform_pixels(pixels: &[(isize, isize, isize)],
        transform: &[f64; 6], coord_transform: &CoordTransform)
        -> Result<WindowBounds, Box<dyn Error>> {
    // convert pixels to coordinates
    let mut xs: Vec<f64> = pixels.iter().map(|(x, y, _)| {
        transform[0] + (*x as f64 * transform[1])
            + (*y as f64 * transform[2])
    }).collect();

    let mut ys: Vec<f64> = pixels.iter().map(|(x, y, _)| {
        transform[3] + (*x as f64 * transform[4])
            + (*y as f64 * transform[5])
    }).collect();

    let mut zs: Vec<f64> = pixels.iter()
        .map(|(_, _, z)| *z as f64).collect();

    // perform coordinate transform
    coord_transform.transform_coords(&mut xs, &mut ys, &mut zs)?;

    Ok((xs, ys, zs))
}

pub fn transform_coord(x: f64, y: f64, z: f64,
        coord_transform: &CoordTransform)
        -> Result<(f64, f64, f64), Box<dyn Error>> {
    // insert items into buffer
    let mut xs = vec!(x);
    let mut ys = vec!(y);
    let mut zs = vec!(z);

    // transfrom coordinates
    coord_transform.transform_coords(&mut xs, &mut ys, &mut zs)?;

    // return values
    Ok((xs[0], ys[0], zs[0]))
}

#[cfg(test)]
mod tests {
    use gdal::spatial_ref::{CoordTransform, SpatialRef};

    const APPLETON_LAT_LONG: (f64, f64) = (-88.4, 44.266667);
    const APPLETON_MERCATOR: (f64, f64) = (-9840642.99, 5506802.68);
    const FORT_COLLINS_LAT_LONG: (f64, f64) = (-105.078056, 40.559167);
    const FORT_COLLINS_MERCATOR: (f64, f64) = (-11697235.69, 4947534.74);

    #[test]
    fn transform_coord() {
        // initialize CoordTransform
        let src_spatial_ref = SpatialRef::from_epsg(4326)
            .expect("initailize geohash SpatialRef");
        let dst_spatial_ref = SpatialRef::from_epsg(3857)
            .expect("initailize quadtile SpatialRef");
    
        #[cfg(major_ge_3)]
        {
            use gdal_sys::OSRAxisMappingStrategy;
            src_spatial_ref.set_axis_mapping_strategy(
                OSRAxisMappingStrategy::OAMS_TRADITIONAL_GIS_ORDER);
            dst_spatial_ref.set_axis_mapping_strategy(
                OSRAxisMappingStrategy::OAMS_TRADITIONAL_GIS_ORDER);
        }

        let coord_transform = CoordTransform::new(&src_spatial_ref,
            &dst_spatial_ref).expect("intiailize CoordTransform");

        // test coordinates
        let result = super::transform_coord(APPLETON_LAT_LONG.0,
            APPLETON_LAT_LONG.1, 0.0, &coord_transform);
        assert!(result.is_ok());

        let coordinates = result.unwrap();
        assert!((coordinates.0 - APPLETON_MERCATOR.0).abs() < 0.01);
        assert!((coordinates.1 - APPLETON_MERCATOR.1).abs() < 0.01);

        let result = super::transform_coord(FORT_COLLINS_LAT_LONG.0,
            FORT_COLLINS_LAT_LONG.1, 0.0, &coord_transform);
        assert!(result.is_ok());

        let coordinates = result.unwrap();
        assert!((coordinates.0 - FORT_COLLINS_MERCATOR.0).abs() < 0.01);
        assert!((coordinates.1 - FORT_COLLINS_MERCATOR.1).abs() < 0.01);
    }

    // TODO - transform pixel

    // TODO - transform pixels

    // TODO - test get_bounds
 
    // TODO - test get_windows
}
