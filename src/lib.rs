use gdal::errors::Error;
use gdal::raster::{Buffer, Dataset, Driver};
use gdal::raster::types::GdalType;
use gdal::spatial_ref::{CoordTransform, SpatialRef};
use gdal_sys::GDALDataType;

pub mod coordinate;
pub mod serialize;
pub mod transform;

pub fn coverage<T: Copy + GdalType + PartialEq>(
        dataset: &Dataset, null_value: T) -> Result<f64, Error> {
    let (width, height) = dataset.size();
    let mut invalid_pixels = vec![true; width * height];

    // iterate over rasterbands
    for i in 0..dataset.count() {
        let rasterband = dataset.rasterband(i + 1)?;

        // read rasterband data into buffer
        let buffer = rasterband.read_as::<T>((0, 0),
            (width, height), (width, height))?;

        // iterate over pixels
        for (i, pixel) in buffer.data.iter().enumerate() {
            if *pixel != null_value {
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

#[cfg(test)]
mod tests {
    /*use gdal::raster::{Dataset, Driver};
    use gdal_sys::GDALDataType;

    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::path::Path;

    #[test]
    fn image_split() {
        let path = Path::new("examples/L1C_T13TDE_A003313_20171024T175403");
        //let path = Path::new("examples/T13TDF_20150821T180236_B01.jp2");

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
            // count pixel values in band
            println!("IMAGE: {}", count);
            count += 1;

            // test image pixel coverage
            let gdal_type = dataset.band_type(1)
                .unwrap_or(GDALDataType::GDT_Byte);
            let coverage = match gdal_type {
                GDALDataType::GDT_Byte =>
                    super::coverage::<u8>(&dataset, 0u8),
                GDALDataType::GDT_UInt16 =>
                    super::coverage::<u16>(&dataset, 0u16),
                _ => unimplemented!(),
            };

            if coverage.unwrap_or(0.0) == 0.0 {
                continue;
            }

            // iterate over rasterbands
            for i in 0..dataset.count() {
                let rasterband = dataset.rasterband(i + 1)
                    .expect("retrieve rasterband");

                // read rasterband data into buffer
                let band_type = rasterband.band_type();

                match band_type {
                    GDALDataType::GDT_Byte => {
                        let buffer = rasterband.read_band_as::<u8>()
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
                        }
                    },
                    GDALDataType::GDT_UInt16 => {
                        let buffer = rasterband.read_band_as::<u16>()
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
                        }
                    },
                    _ => unimplemented!(),
                }
            }

            // copy memory datasets to gtiff files
            dataset.create_copy(&driver,
                &format!("/tmp/st-image-{}.tif", count), None)
                .expect("dataset copy");
        }
    }*/

    /*#[test]
    fn transfer() {
        //let path = Path::new("examples/L1C_T13TDE_A003313_20171024T175403");
        //let path = Path::new("examples/T13TDF_20150821T180236_B01.jp2");
        let path = Path::new("/tmp/st-image-0.tif");

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
        read_dataset.create_copy(&driver, "/tmp/st-image-transfer", None)
            .expect("dataset copy");
    }*/
}
