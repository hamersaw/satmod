use geohash::{self, Coordinate};
use image::{GenericImageView, ImageBuffer, Pixel, SubImage};

mod spatial;

use std::ops::{Deref, DerefMut};

pub trait Splittable<T: Pixel + 'static, U: Deref<Target = [T::Subpixel]> + DerefMut> {
    fn split(&self, precision: u8) -> Vec<StImage<T, Vec<T::Subpixel>>>;
}

pub struct RawImage<T: Pixel + 'static, U: Deref<Target = [T::Subpixel]> + DerefMut> {
    image: ImageBuffer<T, U>,
    lat_min: f64,
    lat_max: f64,
    long_min: f64,
    long_max: f64,
}

impl<T: Pixel + 'static, U: Deref<Target = [T::Subpixel]> + DerefMut> RawImage<T, U> {
    pub fn new(image: ImageBuffer<T, U>, lat_min: f64, lat_max: f64,
            long_min: f64, long_max: f64) -> RawImage<T, U> {
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

impl<T: Pixel + 'static, U: Deref<Target = [T::Subpixel]> + DerefMut + 'static> Splittable<T, U> for RawImage<T, U> {
    fn split(&self, precision: u8) -> Vec<StImage<T, Vec<T::Subpixel>>> {
        // compute geohash coordinate bounds
        let bounds = spatial::get_coordinate_bounds(self.lat_min,
            self.lat_max, self.long_min, self.long_max, precision);

        // iterate over bounds
        let mut st_images = Vec::new();
        for bound in bounds {
            // compute pixels for subimage
            let lat_range = self.lat_max - self.lat_min;
            let min_y = (((bound.0 - self.lat_min) / lat_range)
                * self.image.height() as f64).ceil() as u32;
            let max_y = (((bound.1 - self.lat_min) / lat_range)
                * self.image.height() as f64).floor() as u32;

            let long_range = self.long_max - self.long_min;
            let min_x = (((bound.2 - self.long_min) / long_range)
                * self.image.width() as f64).ceil() as u32;
            let max_x = (((bound.3 - self.long_min) / long_range)
                * self.image.width() as f64).floor() as u32;

            println!("{}-{}, {}-{}", min_y, max_y, min_x, max_x);

            // initialize subimage
            let subimage = &self.image.view(min_x, min_y,
                max_x - min_x, max_y - min_y);
            let image = subimage.to_image();

            // add new StImage
            st_images.push(StImage::new(image,
                bound.0, bound.1, bound.2, bound.3, precision));
        }

        st_images
        //Vec::new()
    }
}

pub struct StImage<T: Pixel + 'static, U: Deref<Target = [T::Subpixel]> + DerefMut> {
    image: ImageBuffer<T, U>,
    lat_min: f64,
    lat_max: f64,
    long_min: f64,
    long_max: f64,
    precision: u8,
}

impl<T: Pixel + 'static, U: Deref<Target = [T::Subpixel]> + DerefMut> StImage<T, U> {
    pub fn new(image: ImageBuffer<T, U>, lat_min: f64, lat_max: f64,
            long_min: f64, long_max: f64, precision: u8) -> StImage<T, U> {
        // TODO - check coordinates for validity
        StImage {
            image: image,
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
