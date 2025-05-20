#!/usr/bin/env node

import fs from 'fs/promises';
import path from 'path';
import pg from 'pg';
import { fileURLToPath } from 'url';

// Command line argument handling
const args = process.argv.slice(2);
if (args.length === 0 || args[0] === '-h' || args[0] === '--help') {
  console.log(`
Usage: node import_bakta.mjs <folder_path> [options]

Options:
  --db-name       PostgreSQL database name (default: bakta_tsv)
  --db-user       PostgreSQL username (default: postgres)
  --db-password   PostgreSQL password
  --db-host       PostgreSQL host (default: localhost)
  --db-port       PostgreSQL port (default: 5432)
  --pattern       File pattern to match TSV files (default: .tsv)

Example:
  node import_bakta.mjs ./data_folder --db-password=mypassword
`);
  process.exit(0);
}

// Parse command line arguments
const folderPath = args[0];
let dbName = 'bakta_tsv';
let dbUser = 'postgres';
let dbPassword = '';
let dbHost = 'localhost';
let dbPort = 5432;
let filePattern = '.tsv';

for (let i = 1; i < args.length; i++) {
  const arg = args[i];
  if (arg.startsWith('--db-name=')) {
    dbName = arg.replace('--db-name=', '');
  } else if (arg.startsWith('--db-user=')) {
    dbUser = arg.replace('--db-user=', '');
  } else if (arg.startsWith('--db-password=')) {
    dbPassword = arg.replace('--db-password=', '');
  } else if (arg.startsWith('--db-host=')) {
    dbHost = arg.replace('--db-host=', '');
  } else if (arg.startsWith('--db-port=')) {
    dbPort = arg.replace('--db-port=', '');
  } else if (arg.startsWith('--pattern=')) {
    filePattern = arg.replace('--pattern=', '');
  }
}

// Connect to PostgreSQL
const pool = new pg.Pool({
  user: dbUser,
  host: dbHost,
  database: dbName,
  password: dbPassword,
  port: dbPort,
});

/**
 * Parse metadata from header lines
 * @param {string[]} lines - Header lines from the TSV file
 * @returns {Object} - Metadata object
 */
function parseMetadata(lines) {
  const metadata = {
    software_version: '',
    database_version: '',
    database_type: '',
    doi: '',
    url: ''
  };

  for (const line of lines) {
    if (line.startsWith('# Software:')) {
      metadata.software_version = line.replace('# Software:', '').trim();
    } else if (line.startsWith('# Database:')) {
      const parts = line.replace('# Database:', '').trim().split(',');
      if (parts.length >= 1) {
        metadata.database_version = parts[0].trim();
      }
      if (parts.length >= 2) {
        metadata.database_type = parts[1].trim();
      }
    } else if (line.startsWith('# DOI:')) {
      metadata.doi = line.replace('# DOI:', '').trim();
    } else if (line.startsWith('# URL:')) {
      metadata.url = line.replace('# URL:', '').trim();
    }
  }

  return metadata;
}

/**
 * Process a single TSV file and import it into the database
 * @param {string} filePath - Path to the TSV file
 */
async function processTsvFile(filePath) {
  console.log(`Processing file: ${filePath}`);
  
  const client = await pool.connect();
  
  try {
    await client.query('BEGIN');
    
    // Read and parse file
    const fileContent = await fs.readFile(filePath, 'utf-8');
    const lines = fileContent.split('\n');
    
    // Separate header and data lines
    const headerLines = lines.filter(line => line.startsWith('#'));
    const dataLines = lines.filter(line => !line.startsWith('#') && line.trim() !== '');
    
    // Extract metadata from header
    const metadata = parseMetadata(headerLines);
    
    // Use filename as sample_id
    const fileName = path.basename(filePath);
    const sampleId = fileName.replace(/\.[^/.]+$/, ''); // Remove file extension
    
    // Insert genome data
    const genomeResult = await client.query(`
      INSERT INTO genomes (sample_id, software_version, database_version, database_type, doi, url, file_path)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING id
    `, [
      sampleId,
      metadata.software_version,
      metadata.database_version,
      metadata.database_type,
      metadata.doi,
      metadata.url,
      filePath
    ]);
    
    const genomeId = genomeResult.rows[0].id;
    
    // Process annotations in batches
    const batchSize = 1000;
    const totalBatches = Math.ceil(dataLines.length / batchSize);
    
    for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
      const start = batchIndex * batchSize;
      const end = Math.min(start + batchSize, dataLines.length);
      const batch = dataLines.slice(start, end);
      
      // Prepare values for batch insert
      const values = [];
      const valueStrings = [];
      let valueIndex = 1;
      
      for (const line of batch) {
        const parts = line.split('\t');
        if (parts.length < 8) continue;
        
        // Parse dbxrefs if available
        const dbxrefs = parts.length > 8 && parts[8] 
          ? parts[8].split(', ').filter(x => x.trim() !== '') 
          : null;
        
        valueStrings.push(`($${valueIndex++}, $${valueIndex++}, $${valueIndex++}, $${valueIndex++}, $${valueIndex++}, $${valueIndex++}, $${valueIndex++}, $${valueIndex++}, $${valueIndex++}, $${valueIndex++})`);
        
        values.push(
          genomeId,
          parts[0], // sequence_id
          parts[1], // feature_type
          parseInt(parts[2]), // start_position
          parseInt(parts[3]), // stop_position
          parts[4], // strand
          parts[5] || null, // locus_tag
          parts[6] || null, // gene
          parts[7] || null, // product
          dbxrefs // dbxrefs
        );
      }
      
      if (valueStrings.length > 0) {
        const query = `
          INSERT INTO annotations 
          (genome_id, sequence_id, feature_type, start_position, stop_position, strand, locus_tag, gene, product, dbxrefs)
          VALUES ${valueStrings.join(', ')}
        `;
        
        await client.query(query, values);
      }
    }
    
    await client.query('COMMIT');
    console.log(`Imported ${dataLines.length} annotations from ${fileName}`);
    
  } catch (error) {
    await client.query('ROLLBACK');
    console.error(`Error processing file ${filePath}:`, error);
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Main function to process all TSV files in a directory
 */
async function main() {
  try {
    // Get all files in the directory
    const files = await fs.readdir(folderPath);
    
    // Filter for TSV files
    const tsvFiles = files.filter(file => file.toLowerCase().includes(filePattern));
    
    if (tsvFiles.length === 0) {
      console.log(`No TSV files found in ${folderPath} matching pattern ${filePattern}`);
      process.exit(1);
    }
    
    console.log(`Found ${tsvFiles.length} TSV files to import`);
    
    // Process each file sequentially to avoid overwhelming the database
    for (let i = 0; i < tsvFiles.length; i++) {
      const filePath = path.join(folderPath, tsvFiles[i]);
      await processTsvFile(filePath);
      console.log(`Processed ${i + 1}/${tsvFiles.length} files`);
    }
    
    console.log('All files processed successfully');
    await pool.end();
    
  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }
}

// Run the main function
main();