use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use gdal::errors::Error;
use gdal::raster::{Buffer, Dataset, Driver};
use gdal::raster::types::GdalType;
use gdal_sys::GDALDataType;

use std::io::{Read, Write};

mod coordinate;
pub mod prelude;
mod serialize;
mod transform;

pub fn coverage(dataset: &Dataset) -> Result<f64, Error> {
    let (width, height) = dataset.size();
    let mut invalid_pixels = vec![true; width * height];
    
    // iterate over rasterbands
    for i in 0..dataset.count() {
        match dataset.band_type(i+1)? {
            GDALDataType::GDT_Byte => _coverage::<u8>(dataset,
                i+1, &mut invalid_pixels, 0u8)?,
            GDALDataType::GDT_Int16 => _coverage::<i16>(dataset,
                i+1, &mut invalid_pixels, 0i16)?,
            GDALDataType::GDT_UInt16 => _coverage::<u16>(dataset,
                i+1, &mut invalid_pixels, 0u16)?,
            _ => unimplemented!(),
        }
    }

    // compute percentage of valid pixels
    let pixel_count = (width * height) as f64;
    let invalid_count = invalid_pixels.iter()
        .filter(|x| **x).count() as f64;

    Ok((pixel_count - invalid_count) / pixel_count)
}

fn _coverage<T: Copy + GdalType + PartialEq>(dataset: &Dataset,
        index: isize, invalid_pixels: &mut Vec<bool>,
        null_value: T) -> Result<(), Error> {
    // read rasterband data into buffer
    let buffer = dataset.read_full_raster_as::<T>(index)?;

    // iterate over pixels
    for (i, pixel) in buffer.data.iter().enumerate() {
        if *pixel != null_value {
            invalid_pixels[i] = false;
        }
    }

    Ok(())
}

pub fn fill(datasets: &Vec<Dataset>) -> Result<Dataset, Error> {
    // TODO - test datatype for each dataset
    match datasets[0].band_type(1)? {
        GDALDataType::GDT_Byte => _fill::<u8>(datasets, 0u8),
        GDALDataType::GDT_Int16 => _fill::<i16>(datasets, 0i16),
        GDALDataType::GDT_UInt16 => _fill::<u16>(datasets, 0u16),
        _ => unimplemented!(),
    }
}

fn _fill<T: Copy + GdalType + PartialEq>(datasets: &Vec<Dataset>,
        null_value: T) -> Result<Dataset, Error> {
    let dataset = &datasets[0];

    // read first dataset rasters
    let mut rasters = Vec::new();
    for i in 0..dataset.count() {
        let raster = dataset.read_full_raster_as::<T>(i + 1).unwrap();
        rasters.push(raster);
    }

    // fill with remaining datasets
    for i in 1..datasets.len() {
        let fill_dataset = &datasets[i];

        // read fill dataset rasterbands
        let mut fill_rasters = Vec::new();
        for j in 0..fill_dataset.count() {
            let fill_raster = fill_dataset
                .read_full_raster_as::<T>(j+1).unwrap();
            fill_rasters.push(fill_raster);
        }

        // iterate over pixels
        let size = rasters[0].data.len();
        for j in 0..size {
            if fill_rasters[0].data.len() <= j {
                break;
            }

            // check if rasterband pixel is valid
            let mut valid = false;
            for k in 0..rasters.len() {
                valid = valid || rasters[k].data[j] != null_value;
            }

            // copy pixels from fill_raster bands
            if !valid {
                for k in 0..rasters.len() {
                    rasters[k].data[j] = fill_rasters[k].data[j];
                }
            }
        }
    }

    // open memory dataset
    let (width, height) = dataset.size();
    let driver = Driver::get("Mem").unwrap();
    let mem_dataset = crate::init_dataset(&driver,
        "unreachable", T::gdal_type(), width as isize,
        height as isize, rasters.len() as isize)?;

    mem_dataset.set_geo_transform(
        &dataset.geo_transform().unwrap()).unwrap();
    mem_dataset.set_projection(
        &dataset.projection()).unwrap();

    // set rasterbands - TODO error
    for (i, raster) in rasters.iter().enumerate() {
        mem_dataset.write_raster::<T>((i + 1) as isize,
            (0, 0), (width, height), &raster).unwrap();
    }

    Ok(mem_dataset)
}

fn init_dataset(driver: &Driver, filename: &str,
        gdal_type: GDALDataType::Type, width: isize, height: isize,
        rasterband_count: isize) -> Result<Dataset, Error> {
    match gdal_type {
        GDALDataType::GDT_Byte => driver.create_with_band_type::<u8>
            (filename, width, height, rasterband_count),
        GDALDataType::GDT_Int16 => driver.create_with_band_type::<i16>
            (filename, width, height, rasterband_count),
        GDALDataType::GDT_UInt16 => driver.create_with_band_type::<u16>
            (filename, width, height, rasterband_count),
        _ => unimplemented!(),
    }
}

fn copy_raster(src_dataset: &Dataset, src_index: isize,
        src_window: (isize, isize), src_window_size: (usize, usize),
        dst_dataset: &Dataset, dst_index: isize, 
        dst_window: (isize, isize), dst_window_size: (usize, usize))
        -> Result<(), Error> {
    match src_dataset.band_type(src_index)? {
        GDALDataType::GDT_Byte => _copy_raster::<u8>(src_dataset, 
            src_index, src_window, src_window_size, dst_dataset, 
            dst_index, dst_window, dst_window_size),
        GDALDataType::GDT_Int16 => _copy_raster::<i16>(src_dataset, 
            src_index, src_window, src_window_size, dst_dataset, 
            dst_index, dst_window, dst_window_size),
        GDALDataType::GDT_UInt16 => _copy_raster::<u16>(src_dataset, 
            src_index, src_window, src_window_size, dst_dataset, 
            dst_index, dst_window, dst_window_size),
        _ => unimplemented!(),
    }
}

fn _copy_raster<T: Copy + GdalType>(src_dataset: &Dataset,
        src_index: isize, src_window: (isize, isize), 
        src_window_size: (usize, usize), dst_dataset: &Dataset,
        dst_index: isize, dst_window: (isize, isize), 
        dst_window_size: (usize, usize)) -> Result<(), Error> {
    // read rasterband data into buffer
    let buffer = src_dataset.read_raster_as::<T>(src_index,
        src_window, src_window_size, dst_window_size)?;

    dst_dataset.write_raster::<T>(dst_index,
        dst_window, dst_window_size, &buffer)?;

    Ok(())
}

fn read_raster<T: Read>(dataset: &Dataset, index: isize,
        reader: &mut T) -> Result<(), Box<dyn std::error::Error>> {
    // compute raster size
    let (width, height) = dataset.size();
    let size = (width * height) as usize;

    // read raster type
    let gdal_type = reader.read_u32::<BigEndian>()?;
    match gdal_type  {
        GDALDataType::GDT_Byte => {
            let mut data = vec![0u8; size];
            reader.read_exact(&mut data)?;

            let buffer = Buffer::new((width as usize,
                height as usize), data);

            dataset.write_raster::<u8>(index, (0, 0), (width as usize,
                height as usize), &buffer).unwrap();
        },
        GDALDataType::GDT_UInt16 => {
            // read rasterband
            let mut data = Vec::new();
            for _ in 0..size {
                data.push(reader.read_i16::<BigEndian>()?);
            }

            let buffer = Buffer::new((width as usize,
                height as usize), data);

            dataset.write_raster::<i16>(index, (0, 0), (width as usize,
                height as usize), &buffer).unwrap();
        },
        GDALDataType::GDT_UInt16 => {
            // read rasterband
            let mut data = Vec::new();
            for _ in 0..size {
                data.push(reader.read_u16::<BigEndian>()?);
            }

            let buffer = Buffer::new((width as usize,
                height as usize), data);

            dataset.write_raster::<u16>(index, (0, 0), (width as usize,
                height as usize), &buffer).unwrap();
        },
        _ => unimplemented!(),
    }

    Ok(())
}

fn write_raster<T: Write>(dataset: &Dataset, index: isize,
        writer: &mut T) -> Result<(), Box<dyn std::error::Error>> {
    // TODO - error
    let gdal_type = dataset.band_type(index).unwrap();
    writer.write_u32::<BigEndian>(gdal_type)?;

    match gdal_type {
        GDALDataType::GDT_Byte => {
            let buffer = dataset
                .read_full_raster_as::<u8>(index).unwrap();
            writer.write(&buffer.data)?;
        },
        GDALDataType::GDT_Int16 => {
            let buffer = dataset
                .read_full_raster_as::<i16>(index).unwrap();
            for pixel in buffer.data {
                writer.write_i16::<BigEndian>(pixel)?;
            }
        },
        GDALDataType::GDT_UInt16 => {
            let buffer = dataset
                .read_full_raster_as::<u16>(index).unwrap();
            for pixel in buffer.data {
                writer.write_u16::<BigEndian>(pixel)?;
            }
        },
        _ => unimplemented!(),
    }

    Ok(())
}
