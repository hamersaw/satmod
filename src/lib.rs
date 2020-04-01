use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use geohash::{self, Coordinate};
use image::{Bgr, DynamicImage, GenericImageView, ImageBuffer, Rgb};

use std::error::Error;
use std::io::{Read, Write};

mod spatial;

pub struct StImage {
    image: DynamicImage,
    lat_min: f64,
    lat_max: f64,
    long_min: f64,
    long_max: f64,
    precision: Option<usize>,
}

impl StImage {
    pub fn new(image: DynamicImage, lat_min: f64, lat_max: f64,
            long_min: f64, long_max: f64, precision: Option<usize>) -> StImage {
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

    pub fn coverage(&self) -> Option<f64> {
        match self.coverage_spatial() {
            Some(coverage_spatial) => 
                Some(coverage_spatial * self.coverage_pixel()),
            None => None,
        }
    }

    pub fn coverage_pixel(&self) -> f64 {
        // write image
        let valid_pixels = match &self.image {
            DynamicImage::ImageLuma8(_image) => {
                println!("TODO - implement ImageLuma8");
                unimplemented!();
            },
            DynamicImage::ImageLumaA8(_image) => {
                println!("TODO - implement ImageLumaA8");
                unimplemented!();
            },
            DynamicImage::ImageRgb8(image) => {
                let black = Rgb([255u8, 255u8, 255u8]);
                let white = Rgb([255u8, 255u8, 255u8]);

                image.pixels().filter(|&x| {
                    x.ne(&black) && x.ne(&white)
                }).count()
            },
            DynamicImage::ImageRgba8(_image) => {
                println!("TODO - implement ImageRgba8");
                unimplemented!();
            },
            DynamicImage::ImageBgr8(_image) => {
                println!("TODO - implement ImageBgr8");
                unimplemented!();
            },
            DynamicImage::ImageBgra8(_image) => {
                println!("TODO - implement ImageBgra8");
                unimplemented!();
            },
            DynamicImage::ImageLuma16(_image) => {
                println!("TODO - implement ImageLuma16");
                unimplemented!();
            },
            DynamicImage::ImageLumaA16(_image) => {
                println!("TODO - implement ImageLumaA16");
                unimplemented!();
            },
            DynamicImage::ImageRgb16(_image) => {
                println!("TODO - implement ImageRgb16");
                unimplemented!();
            },
            DynamicImage::ImageRgba16(_image) => {
                println!("TODO - implement ImageRgba16");
                unimplemented!();
            },
        };

        valid_pixels as f64 /
            (self.image.width() * self.image.height()) as f64
    }

    pub fn coverage_spatial(&self) -> Option<f64> {
        match self.precision {
            None => None,
            Some(_) => {
                let geohash = self.geohash().unwrap();
                let rect = geohash::decode_bbox(&geohash).unwrap();
                let coverage = ((self.long_max - self.long_min)
                    * (self.lat_max - self.lat_min))
                    / (rect.width() * rect.height());

                Some(coverage)
            }
        }
    }

    pub fn get_image(&self) -> &DynamicImage {
        &self.image
    }

    pub fn geohash(&self) -> Option<String> {
        match self.precision {
            None => None,
            Some(precision) => {
                let coordinate = Coordinate{x: self.long_max, y: self.lat_max};
                let geohash = geohash::encode(coordinate, precision);

                Some(geohash.unwrap())
            },
        }
    }

    pub fn read<S: Read>(reader: &mut S)
            -> Result<StImage, Box<dyn Error>> {
        let (lat_min, lat_max, long_min, long_max, precision) =
            StImage::read_metadata(reader)?;
        let image = StImage::read_image(reader)?;

        Ok(StImage::new(image, lat_min, lat_max,
            long_min, long_max, precision))
    }

    pub fn read_image<S: Read>(reader: &mut S)
            -> Result<DynamicImage, Box<dyn Error>> {
        // read DynamicImage
        let height = reader.read_u32::<BigEndian>()?;
        let width = reader.read_u32::<BigEndian>()?;

        let image = match reader.read_u8()? {
            0 => {
                let mut container = vec![0u8; (height * width * 3) as usize];
                reader.read_exact(&mut container)?;

                let image_buffer: ImageBuffer<Rgb<u8>, Vec<u8>> =
                    ImageBuffer::from_raw(width, height, container).unwrap();
                DynamicImage::ImageRgb8(image_buffer)
            },
            1 => {
                let mut container = vec![0u8; (height * width * 3) as usize];
                reader.read_exact(&mut container)?;

                let image_buffer: ImageBuffer<Bgr<u8>, Vec<u8>> =
                    ImageBuffer::from_raw(width, height, container).unwrap();
                DynamicImage::ImageBgr8(image_buffer)
            },
            _ => unimplemented!(),
        };

        Ok(image)
    }

    pub fn read_metadata<S: Read>(reader: &mut S)
            -> Result<(f64, f64, f64, f64, Option<usize>), Box<dyn Error>> {
        // read latitude and longitude bounds
        let lat_min = reader.read_f64::<BigEndian>()?;
        let lat_max = reader.read_f64::<BigEndian>()?;
        let long_min = reader.read_f64::<BigEndian>()?;
        let long_max = reader.read_f64::<BigEndian>()?;

        // read precision
        let precision = match reader.read_u8()? {
            0 => None,
            _ => Some(reader.read_u8()? as usize),
        };

        Ok((lat_min, lat_max, long_min, long_max, precision))
    }

    pub fn split(&mut self, precision: usize) -> Vec<StImage> {
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

            // crop image to geohash bounds
            let subimage = self.image.crop(min_x, min_y,
                max_x - min_x, max_y - min_y);

            // add new StImage
            st_images.push(StImage::new(subimage,
                bound.0, bound.1, bound.2, bound.3, Some(precision)));
        }

        st_images
    }

    pub fn write<S: Write>(&self, writer: &mut S)
            -> Result<(), Box<dyn Error>> {
        self.write_metadata(writer)?;
        self.write_image(writer)?;
        Ok(())
    }

    pub fn write_image<S: Write>(&self, writer: &mut S)
            -> Result<(), Box<dyn Error>> {
        // write dimensions
        writer.write_u32::<BigEndian>(self.image.height())?;
        writer.write_u32::<BigEndian>(self.image.width())?;

        // write image
        match &self.image {
            DynamicImage::ImageLuma8(_image) => {
                println!("TODO - implement ImageLuma8");
                unimplemented!();
            },
            DynamicImage::ImageLumaA8(_image) => {
                println!("TODO - implement ImageLumaA8");
                unimplemented!();
            },
            DynamicImage::ImageRgb8(image) => {
                writer.write_u8(0)?;
                for pixel in image.pixels() {
                    writer.write_u8(pixel[0])?;
                    writer.write_u8(pixel[1])?;
                    writer.write_u8(pixel[2])?;
                }
            },
            DynamicImage::ImageRgba8(_image) => {
                println!("TODO - implement ImageRgba8");
                unimplemented!();
            },
            DynamicImage::ImageBgr8(image) => {
                writer.write_u8(1)?;
                for pixel in image.pixels() {
                    writer.write_u8(pixel[0])?;
                    writer.write_u8(pixel[1])?;
                    writer.write_u8(pixel[2])?;
                }
            },
            DynamicImage::ImageBgra8(_image) => {
                println!("TODO - implement ImageBgra8");
                unimplemented!();
            },
            DynamicImage::ImageLuma16(_image) => {
                println!("TODO - implement ImageLuma16");
                unimplemented!();
            },
            DynamicImage::ImageLumaA16(_image) => {
                println!("TODO - implement ImageLumaA16");
                unimplemented!();
            },
            DynamicImage::ImageRgb16(_image) => {
                println!("TODO - implement ImageRgb16");
                unimplemented!();
            },
            DynamicImage::ImageRgba16(_image) => {
                println!("TODO - implement ImageRgba16");
                unimplemented!();
            },
        }

        Ok(())
    }

    pub fn write_metadata<S: Write>(&self, writer: &mut S)
            -> Result<(), Box<dyn Error>> {
        // write latitude and longitude bounds
        writer.write_f64::<BigEndian>(self.lat_min)?;
        writer.write_f64::<BigEndian>(self.lat_max)?;
        writer.write_f64::<BigEndian>(self.long_min)?;
        writer.write_f64::<BigEndian>(self.long_max)?;

        // write precision
        match self.precision {
            Some(precision) => {
                writer.write_u8(1)?;
                writer.write_u8(precision as u8)?;
            },
            None => writer.write_u8(0)?,
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use image::{self, GenericImageView};
    use super::StImage;

    /*#[test]
    fn image_split() {
        // read jpg image
        let image = image::open("examples/LM01_L1GS_036032_19730622_20180428_01_T2.jpg").unwrap();
        //let image = match image::open("examples/L1C_T13TDE_A022303_20190929T175231-0.png").unwrap();

        let mut raw_image = StImage::new(image,
            39.41291, 41.34748, -106.61415, -103.92836, None);
        for st_image in raw_image.split(4) {
            // TODO - how to test?
            //println!("{:?} - {:?}", st_image.geohash(), st_image.coverage());

            // write image
            //st_image.image.save_with_format(format!("examples/{}{}.png",
            //    st_image.lat_min, st_image.long_min), ImageFormat::Png);
        }
    }*/

    #[test]
    fn image_transfer() {
        let image = image::open("examples/LM01_L1GS_036032_19730622_20180428_01_T2.jpg").unwrap();

        let st_image = StImage::new(image,
            39.41291, 41.34748, -106.61415, -103.92836, None);
        
        // write raw image to vector
        let mut vec = Vec::new();
        st_image.write_image(&mut vec).expect("write image");

        // read new image from vector
        let mut cursor = std::io::Cursor::new(vec);
        let image = StImage::read_image(&mut cursor).expect("read image");

        assert_eq!(st_image.image.width(), image.width());
        assert_eq!(st_image.image.height(), image.height());
    }

    #[test]
    fn metadata_transfer() {
        let image = image::open("examples/LM01_L1GS_036032_19730622_20180428_01_T2.jpg").unwrap();

        let lat_min = 39.41291;
        let lat_max = 41.34748;
        let long_min = -106.61415;
        let long_max = -103.92836;
        let precision = None;

        let st_image = StImage::new(image,
            lat_min, lat_max, long_min, long_max, precision);
        
        // write raw image to vector
        let mut vec = Vec::new();
        st_image.write_metadata(&mut vec).expect("write metadata");

        // read new image from vector
        let mut cursor = std::io::Cursor::new(vec);
        let (dlat_min, dlat_max, dlong_min, dlong_max, dprecision) =
            StImage::read_metadata(&mut cursor).expect("read metadata");

        assert_eq!(lat_min, dlat_min);
        assert_eq!(lat_max, dlat_max);
        assert_eq!(long_min, dlong_min);
        assert_eq!(long_max, dlong_max);
        assert_eq!(precision, dprecision);
    }
}
