use geohash::{self, Coordinate};
use image::DynamicImage;

mod spatial;

pub trait Splittable {
    fn split(&self, precision: u8) -> Vec<StImage>;
}

pub struct RawImage {
    image: DynamicImage,
    lat_min: f64,
    lat_max: f64,
    long_min: f64,
    long_max: f64,
}

impl RawImage {
    pub fn new(image: DynamicImage, lat_min: f64, lat_max: f64,
            long_min: f64, long_max: f64) -> RawImage {
        // TODO - check coordinates for validity
        RawImage {
            image: image,
            lat_min: lat_min,
            lat_max: lat_max,
            long_min: long_min,
            long_max: long_max,
        }
    }
}

impl Splittable for RawImage {
    fn split(&self, precision: u8) -> Vec<StImage> {
        // retrieve image dimensions
        let (x_dim, y_dim) = match &self.image {
            DynamicImage::ImageLuma8(x) => x.dimensions(),
            DynamicImage::ImageLumaA8(x) => x.dimensions(),
            DynamicImage::ImageRgb8(x) => x.dimensions(),
            DynamicImage::ImageRgba8(x) => x.dimensions(),
            DynamicImage::ImageBgr8(x) => x.dimensions(),
            DynamicImage::ImageBgra8(x) => x.dimensions(),
            DynamicImage::ImageLuma16(x) => x.dimensions(),
            DynamicImage::ImageLumaA16(x) => x.dimensions(),
            DynamicImage::ImageRgb16(x) => x.dimensions(),
            DynamicImage::ImageRgba16(x) => x.dimensions(),
        };

        // compute geohash coordinate bounds
        let bounds = spatial::get_coordinate_bounds(self.lat_min,
            self.lat_max, self.long_min, self.long_max, precision);

        // iterate over bounds
        let mut st_images = Vec::new();
        for bound in bounds {
            // compute pixels for image

            let lat_range = self.lat_max - self.lat_min;
            let min_y = (((bound.0 - self.lat_min) / lat_range)
                * y_dim as f64).ceil();
            let max_y = (((bound.1 - self.lat_min) / lat_range)
                * y_dim as f64).floor();

            let long_range = self.long_max - self.long_min;
            let min_x = (((bound.2 - self.long_min) / long_range)
                * x_dim as f64).ceil();
            let max_x = (((bound.3 - self.long_min) / long_range)
                * x_dim as f64).floor();

            println!("{}-{}, {}-{}", min_y, max_y, min_x, max_x);

            // add new StImage
            st_images.push(StImage::new(bound.0, bound.1,
                bound.2, bound.3, precision));
        }

        st_images
    }
}

pub struct StImage {
    lat_min: f64,
    lat_max: f64,
    long_min: f64,
    long_max: f64,
    precision: u8,
}

impl StImage {
    pub fn new(lat_min: f64, lat_max: f64,
            long_min: f64, long_max: f64, precision: u8) -> StImage {
        // TODO - check coordinates for validity
        StImage {
            lat_min: lat_min,
            lat_max: lat_max,
            long_min: long_min,
            long_max: long_max,
            precision: precision,
        }
    }

    pub fn geohash(&self) -> String {
        let geohash = geohash::encode(
            Coordinate{x: self.long_max, y: self.lat_max},
            self.precision as usize
        );

        geohash.unwrap()
    }

    pub fn geohash_coverage(&self) -> f64 {
        let rect = geohash::decode_bbox(&self.geohash()).unwrap();
        ((self.long_max - self.long_min) * (self.lat_max - self.lat_min))
            / (rect.width() * rect.height())
    }
}

#[cfg(test)]
mod tests {
    use image::{self};
    use super::{RawImage, Splittable};

    #[test]
    fn images() {
        // read jpg image
        let image = image::open("examples/LM01_L1GS_036032_19730622_20180428_01_T2.jpg").unwrap();

        let raw_image = RawImage::new(image, 39.41291, 41.34748, -106.61415, -103.92836);
        for st_image in raw_image.split(4) {
            println!("{} - {}", st_image.geohash(),
                st_image.geohash_coverage());
        }
    }
}
