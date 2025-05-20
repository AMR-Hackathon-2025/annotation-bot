#!/usr/bin/env node

import fs from 'fs/promises';
import path from 'path';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import { fileURLToPath } from 'url';

// Command line argument handling
const args = process.argv.slice(2);
if (args.length === 0 || args.includes('-h') || args.includes('--help')) {
  console.log(`
Usage: node import_bakta_sqlite.mjs <folder_path> [options]

Options:
  --db-path       SQLite database file path (default: bakta_annotations.db)
  --pattern       File pattern to match TSV files (default: .tsv)

Example:
  node import_bakta_sqlite.mjs ./data_folder --db-path=my_database.db
`);
  process.exit(0);
}

// Parse command line arguments
const folderPath = args[0];
let dbPath = 'bakta_annotations.db';
let filePattern = '.tsv';

for (let i = 1; i < args.length; i++) {
  const arg = args[i];
  if (arg.startsWith('--db-path=')) {
    dbPath = arg.replace('--db-path=', '');
  } else if (arg.startsWith('--pattern=')) {
    filePattern = arg.replace('--pattern=', '');
  }
}

/**
 * Initialize the SQLite database with required tables
 * @param {Object} db - SQLite database connection
 */
async function initializeDatabase(db) {
  // Check if tables already exist
  const tablesExist = await db.get(
    "SELECT name FROM sqlite_master WHERE type='table' AND name='genomes'"
  );

  if (!tablesExist) {
    console.log('Creating database schema...');
    
    // Create genomes table
    await db.exec(`
      CREATE TABLE genomes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sample_id TEXT,
        software_version TEXT,
        database_version TEXT,
        database_type TEXT,
        doi TEXT,
        url TEXT,
        file_path TEXT,
        import_date TEXT DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Create annotations table
    await db.exec(`
      CREATE TABLE annotations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        genome_id INTEGER,
        sequence_id TEXT,
        feature_type TEXT,
        start_position INTEGER,
        stop_position INTEGER,
        strand TEXT,
        locus_tag TEXT,
        gene TEXT,
        product TEXT,
        dbxrefs TEXT,
        FOREIGN KEY (genome_id) REFERENCES genomes (id)
      )
    `);

    // Create indexes for better query performance
    await db.exec(`
      CREATE INDEX idx_annotations_sequence_id ON annotations(sequence_id);
      CREATE INDEX idx_annotations_feature_type ON annotations(feature_type);
      CREATE INDEX idx_annotations_locus_tag ON annotations(locus_tag);
      CREATE INDEX idx_annotations_gene ON annotations(gene);
    `);
  }
}

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
 * @param {Object} db - SQLite database connection
 * @param {string} filePath - Path to the TSV file
 */
async function processTsvFile(db, filePath) {
  console.log(`Processing file: ${filePath}`);
  
  try {
    // Read and parse file
    const fileContent = await fs.readFile(filePath, 'utf-8');
    const lines = fileContent.split('\n');
    
    // Separate header and data lines
    const headerLines = lines.filter(line => line.startsWith('#'));
    const dataLines = lines.filter(line => !line.startsWith('#') && line.trim() !== '');
    
    // Extract metadata from header
    const metadata = parseMetadata(headerLines);
    
    // Use filename as sample_id (without extension)
    const fileName = path.basename(filePath);
    const sampleId = fileName.replace(/\.[^/.]+$/, '');
    
    await db.run('BEGIN TRANSACTION');
    
    // Insert genome data
    const result = await db.run(`
      INSERT INTO genomes (sample_id, software_version, database_version, database_type, doi, url, file_path)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `, [
      sampleId,
      metadata.software_version,
      metadata.database_version,
      metadata.database_type,
      metadata.doi,
      metadata.url,
      filePath
    ]);
    
    const genomeId = result.lastID;
    
    // Process annotations in batches
    const batchSize = 500; // Smaller batch size for SQLite
    const totalBatches = Math.ceil(dataLines.length / batchSize);
    
    const insertStmt = await db.prepare(`
      INSERT INTO annotations 
      (genome_id, sequence_id, feature_type, start_position, stop_position, strand, locus_tag, gene, product, dbxrefs)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    
    for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
      const start = batchIndex * batchSize;
      const end = Math.min(start + batchSize, dataLines.length);
      const batch = dataLines.slice(start, end);
      
      for (const line of batch) {
        const parts = line.split('\t');
        if (parts.length < 8) continue;
        
        // Join dbxrefs as a JSON string for SQLite
        const dbxrefs = parts.length > 8 && parts[8] 
          ? JSON.stringify(parts[8].split(', ').filter(x => x.trim() !== '')) 
          : null;
        
        await insertStmt.run(
          genomeId,
          parts[0], // sequence_id
          parts[1], // feature_type
          parseInt(parts[2]), // start_position
          parseInt(parts[3]), // stop_position
          parts[4], // strand
          parts[5] || null, // locus_tag
          parts[6] || null, // gene
          parts[7] || null, // product
          dbxrefs // dbxrefs as JSON
        );
      }
    }
    
    await insertStmt.finalize();
    await db.run('COMMIT');
    
    console.log(`Imported ${dataLines.length} annotations from ${fileName}`);
    
  } catch (error) {
    await db.run('ROLLBACK');
    console.error(`Error processing file ${filePath}:`, error);
    throw error;
  }
}

/**
 * Main function to process all TSV files in a directory
 */
async function main() {
  try {
    // Open SQLite database connection
    const db = await open({
      filename: dbPath,
      driver: sqlite3.Database
    });
    
    // Initialize database schema
    await initializeDatabase(db);
    
    // Get all files in the directory
    const files = await fs.readdir(folderPath);
    
    // Filter for TSV files
    const tsvFiles = files.filter(file => file.toLowerCase().includes(filePattern));
    
    if (tsvFiles.length === 0) {
      console.log(`No TSV files found in ${folderPath} matching pattern ${filePattern}`);
      process.exit(1);
    }
    
    console.log(`Found ${tsvFiles.length} TSV files to import into ${dbPath}`);
    
    let successCount = 0;
    let errorCount = 0;
    
    // Process each file sequentially
    for (let i = 0; i < tsvFiles.length; i++) {
      const filePath = path.join(folderPath, tsvFiles[i]);
      try {
        await processTsvFile(db, filePath);
        successCount++;
      } catch (error) {
        errorCount++;
        // Continue with next file
      }
      console.log(`Processed ${i + 1}/${tsvFiles.length} files`);
    }
    
    console.log(`Import completed: ${successCount} files successful, ${errorCount} files failed`);
    
    // Close the database
    await db.close();
    
  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  }
}

// Run the main function
main();