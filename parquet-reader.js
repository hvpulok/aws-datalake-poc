const { ParquetReader } = require('parquetjs-lite');

async function readParquetFile(filePath) {
  let reader;
  try {
    // Create a new ParquetReader instance for the given file
    reader = await ParquetReader.openFile(filePath);

    // Create a cursor that will read through the file
    const cursor = reader.getCursor();

    let record = null;
    // Read each record in the file and log it
    while (record = await cursor.next()) {
      console.log(record);
    }
  } catch (error) {
    console.error('Error reading Parquet file:', error);
  } finally {
    if (reader) {
      // Close the parquet reader
      await reader.close();
    }
  }
}

// Replace 'path/to/your/file.parquet' with the actual path to your Parquet file
readParquetFile('./data/user-info.parquet');
