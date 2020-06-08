use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use gdal::raster::{Dataset, Driver};
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
    for i in 0..rasterband_count {
        crate::read_raster(&dataset, i+1, reader)?;
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
        crate::write_raster(dataset, i+1, writer)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use gdal::raster::{Dataset, Driver};
    use gdal_sys::GDALDataType;

    use std::collections::BTreeMap;
    use std::io::Cursor;
    use std::path::Path;

    /*#[test]
    fn transfer() {
        let path = Path::new("examples/L1C_T13TDE_A003313_20171024T175403");
        //let path = Path::new("examples/T13TDF_20150821T180236_B01.jp2");

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
