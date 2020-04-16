use gdal::errors::Error;
use gdal::spatial_ref::{CoordTransform, SpatialRef};

pub struct StCoordTransform {
    coord_transform: CoordTransform,
    transform: [f64; 6],
}

impl StCoordTransform {
    pub fn new(transform: [f64; 6], src_projection: &str,
            dst_projection: u32) -> Result<StCoordTransform, Error> {
        let src_spatial_ref = SpatialRef::from_wkt(src_projection)?;
        let dst_spatial_ref = SpatialRef::from_epsg(dst_projection)?;
        let coord_transform = 
            CoordTransform::new(&src_spatial_ref, &dst_spatial_ref)?;

        Ok(StCoordTransform {
            coord_transform: coord_transform,
            transform: transform,
        })
    }

    pub fn transform_pixel(&self, x: usize, y: usize, z: usize)
            -> Result<(f64, f64, f64), Error> {
        let x_coord = self.transform[0] + (x as f64 * self.transform[1])
            + (y as f64 * self.transform[2]);
        let y_coord = self.transform[3] + (x as f64 * self.transform[4])
            + (y as f64 * self.transform[5]);

        self.transform_coord(x_coord, y_coord, z as f64)
    }

    pub fn transform_pixels(&self, pixels: &Vec<(usize, usize, usize)>)
            -> Result<(Vec<f64>, Vec<f64>, Vec<f64>), Error> {
        // convert pixels to coordinates
        let mut xs: Vec<f64> = pixels.iter().map(|(x, y, _)| {
            self.transform[0] + (*x as f64 * self.transform[1])
                + (*y as f64 * self.transform[2])
        }).collect();

        let mut ys: Vec<f64> = pixels.iter().map(|(x, y, _)| {
            self.transform[3] + (*x as f64 * self.transform[4])
                + (*y as f64 * self.transform[5])
        }).collect();

        let mut zs = pixels.iter()
            .map(|(_, _, z)| *z as f64).collect();

        // perform coordinate transform
        self.transform_coords(&mut xs, &mut ys, &mut zs)?;

        Ok((xs, ys, zs))
    }

    pub fn transform_coord(&self, x: f64, y: f64, z: f64)
            -> Result<(f64, f64, f64), Error> {
        // insert items into buffer
        let mut xs = vec!(x);
        let mut ys = vec!(y);
        let mut zs = vec!(z);

        // transfrom coordinates
        self.transform_coords(&mut xs, &mut ys, &mut zs)?;

        // return values
        Ok((xs[0], ys[0], zs[0]))
    }

    pub fn transform_coords(&self, xs: &mut Vec<f64>, ys: &mut Vec<f64>,
            zs: &mut Vec<f64>) -> Result<(), Error> {
        // transfrom coordinates
        self.coord_transform.transform_coords(xs, ys, zs)
    }
}

#[cfg(test)]
mod tests {
    use gdal::raster::{Dataset, Driver};

    use super::StCoordTransform;

    use std::path::Path;

    #[test]
    fn pixel_transform() {
        // read dataset
        let path = Path::new("examples/L1C_T13TDE_A003313_20171024T175403");
        let dataset = Dataset::open(path).expect("dataset open");

        // initialize coordinate transform
        let mut coord_transform = StCoordTransform::new(
            dataset.geo_transform().expect("get transform"),
            &dataset.projection(), 4326).expect("init StCoordTransform");

        // transform pixels
        //let mut pixels = Vec::new();

        let (width, height) = dataset.size();
        for i in 0..width {
            for j in 0..height {
                //pixels.push((i, j, 0));
                //let (x, y, z) = coord_transform.transform_pixel(i, j, 0)
                //    .expect("transform pixel");
                //println!("{} {} - {} {} {}", i, j, x, y, z);
            }
        }

        //let (xs, ys, zs) = coord_transform.transform_pixels(&pixels)
        //    .expect("transform pixels");
    }
}
