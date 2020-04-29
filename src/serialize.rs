use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use gdal::raster::{Buffer, Dataset, Driver};
use gdal_sys::GDALDataType;

use std::io::{Read, Write};

pub fn read<T: Read>(reader: &mut T)
        -> Result<Dataset, Box<dyn std::error::Error>> {
    // read image dimensions
    let width = reader.read_u32::<BigEndian>()? as isize;
    let height = reader.read_u32::<BigEndian>()? as isize;

    // read geo transform
    let mut transform = [0.0f64; 6];
    for i in 0..transform.len() {
        transform[i] = reader.read_f64::<BigEndian>()?;
    }
 
    // read projection
    let projection_len = reader.read_u32::<BigEndian>()?;
    let mut projection_buf = vec![0u8; projection_len as usize];
    reader.read_exact(&mut projection_buf)?;
    let projection = String::from_utf8(projection_buf)?;

    // read gdal type
    let gdal_type = reader.read_u32::<BigEndian>()?;
 
    // read rasterband count
    let rasterband_count = reader.read_u8()? as isize;

    // initialize dataset - TODO error
    let driver = Driver::get("Mem").unwrap();
    let dataset = crate::init_dataset(&driver, "unreachable",
        gdal_type, width, height, rasterband_count).unwrap();

    dataset.set_geo_transform(&transform).unwrap();
    dataset.set_projection(&projection).unwrap();
 
    // read rasterbands
    let size = (width * height) as usize;
    for i in 0..rasterband_count {
        match gdal_type {
            GDALDataType::GDT_Byte => {
                // read rasterband
                let mut data = vec![0u8; size];
                reader.read_exact(&mut data)?;

                let buffer = Buffer::new((width as usize,
                    height as usize), data);

                dataset.write_raster::<u8>(i+1, (0, 0), (width as usize,
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

                dataset.write_raster::<u16>(i+1, (0, 0), (width as usize,
                    height as usize), &buffer).unwrap();
            },
            _ => unimplemented!(),
        }
    }

    Ok(dataset)
}

pub fn write<T: Write>(dataset: &Dataset, writer: &mut T)
        -> Result<(), Box<dyn std::error::Error>> {
    // write image dimensions
    let (width, height) = dataset.size();
    writer.write_u32::<BigEndian>(width as u32)?;
    writer.write_u32::<BigEndian>(height as u32)?;

    // write geo transform - TODO error
    let transform = dataset.geo_transform().unwrap();
    for val in transform.iter() {
        writer.write_f64::<BigEndian>(*val)?;
    }

    // write projection
    let projection = dataset.projection();
    writer.write_u32::<BigEndian>(projection.len() as u32)?;
    writer.write(projection.as_bytes())?;

    // write gdal type
    let gdal_type = dataset.band_type(1)
        .unwrap_or(GDALDataType::GDT_Byte);
    writer.write_u32::<BigEndian>(gdal_type)?;

    // write rasterbands
    writer.write_u8(dataset.count() as u8)?;
    for i in 0..dataset.count() {
        // TODO - error
        match gdal_type {
            GDALDataType::GDT_Byte => {
                // writer rasterband data
                let rasterband =
                    dataset.read_full_raster_as::<u8>(i + 1).unwrap();

                writer.write(&rasterband.data)?;
            },
            GDALDataType::GDT_UInt16 => {
                // writer rasterband data
                let rasterband =
                    dataset.read_full_raster_as::<u16>(i + 1).unwrap();

                for pixel in rasterband.data {
                    writer.write_u16::<BigEndian>(pixel)?;
                }
            },
            _ => unimplemented!(),
        }
    }

    Ok(())
}
