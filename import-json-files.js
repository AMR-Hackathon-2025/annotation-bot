const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const { promisify } = require('util');
const readdir = promisify(fs.readFile);
const readFile = promisify(fs.readFile);

// Database connection configuration
const pool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'bakta_annotations_all',
  password: 'postgres', // Replace with your actual password
  port: 5432,
});

/**
 * Insert a feature and its related data into the database
 * @param {Object} feature - Feature object from JSON
 * @param {number} genomeId - ID of the genome in the database
 * @param {Object} client - PostgreSQL client for transaction
 */
async function insertFeature(feature, genomeId, client) {
  // Insert basic feature data
  const featureResult = await client.query(
    `INSERT INTO features 
      (genome_id, feature_id, type, contig, start, stop, strand, frame, gene, product, 
       start_type, rbs_motif, locus) 
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) 
     RETURNING id`,
    [
      genomeId,
      feature.id || null,
      feature.type || null,
      feature.contig || null,
      feature.start || null,
      feature.stop || null,
      feature.strand || null,
      feature.frame || null,
      feature.gene || null,
      feature.product || null,
      feature.start_type || null,
      feature.rbs_motif || null,
      feature.locus || null
    ]
  );
  
  const featureId = featureResult.rows[0].id;
  
  // Insert db_xrefs if they exist
  if (feature.db_xrefs && Array.isArray(feature.db_xrefs)) {
    for (const dbXref of feature.db_xrefs) {
      await client.query(
        'INSERT INTO db_xrefs (feature_id, db_xref) VALUES ($1, $2)',
        [featureId, dbXref]
      );
    }
  }
  
  // Insert genes if they exist
  if (feature.genes && Array.isArray(feature.genes)) {
    for (const gene of feature.genes) {
      await client.query(
        'INSERT INTO genes (feature_id, gene) VALUES ($1, $2)',
        [featureId, gene]
      );
    }
  }
  
  // Insert UPS data if it exists
  if (feature.ups) {
    const upsResult = await client.query(
      `INSERT INTO ups (feature_id, uniparc_id, ncbi_nrp_id, uniref100_id)
       VALUES ($1, $2, $3, $4)
       RETURNING id`,
      [
        featureId,
        feature.ups.uniparc_id || null,
        feature.ups.ncbi_nrp_id || null,
        feature.ups.uniref100_id || null
      ]
    );
    
    const upsId = upsResult.rows[0].id;
    
    // Insert ups db_xrefs if they exist
    if (feature.ups.db_xrefs && Array.isArray(feature.ups.db_xrefs)) {
      for (const dbXref of feature.ups.db_xrefs) {
        await client.query(
          'INSERT INTO ups_db_xrefs (ups_id, db_xref) VALUES ($1, $2)',
          [upsId, dbXref]
        );
      }
    }
  }
  
  // Insert IPS data if it exists
  if (feature.ips) {
    await client.query(
      `INSERT INTO ips (feature_id, uniref100_id, uniref90_id)
       VALUES ($1, $2, $3)`,
      [
        featureId,
        feature.ips.uniref100_id || null,
        feature.ips.uniref90_id || null
      ]
    );
  }
  
  // Insert PSC data if it exists
  if (feature.psc) {
    const pscResult = await client.query(
      `INSERT INTO psc (feature_id, uniref90_id, gene, product, uniref50_id, cog_id, cog_category)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       RETURNING id`,
      [
        featureId,
        feature.psc.uniref90_id || null,
        feature.psc.gene || null,
        feature.psc.product || null,
        feature.psc.uniref50_id || null,
        feature.psc.cog_id || null,
        feature.psc.cog_category || null
      ]
    );
    
    const pscId = pscResult.rows[0].id;
    
    // Insert GO IDs if they exist
    if (feature.psc.go_ids && Array.isArray(feature.psc.go_ids)) {
      for (const goId of feature.psc.go_ids) {
        await client.query(
          'INSERT INTO psc_go_ids (psc_id, go_id) VALUES ($1, $2)',
          [pscId, goId]
        );
      }
    }
    
    // Insert EC IDs if they exist
    if (feature.psc.ec_ids && Array.isArray(feature.psc.ec_ids)) {
      for (const ecId of feature.psc.ec_ids) {
        await client.query(
          'INSERT INTO psc_ec_ids (psc_id, ec_id) VALUES ($1, $2)',
          [pscId, ecId]
        );
      }
    }
  }
  
  // Insert PSCC data if it exists
  if (feature.pscc) {
    const psccResult = await client.query(
      `INSERT INTO pscc (feature_id, uniref50_id, product)
       VALUES ($1, $2, $3)
       RETURNING id`,
      [
        featureId,
        feature.pscc.uniref50_id || null,
        feature.pscc.product || null
      ]
    );
    
    const psccId = psccResult.rows[0].id;
    
    // Insert pscc db_xrefs if they exist
    if (feature.pscc.db_xrefs && Array.isArray(feature.pscc.db_xrefs)) {
      for (const dbXref of feature.pscc.db_xrefs) {
        await client.query(
          'INSERT INTO pscc_db_xrefs (pscc_id, db_xref) VALUES ($1, $2)',
          [psccId, dbXref]
        );
      }
    }
  }
}

/**
 * Process a single JSON file and insert its features into the database
 * @param {string} filePath - Path to the JSON file
 */
async function processFile(filePath) {
  console.log(`Processing file: ${filePath}`);
  try {
    const fileContent = await readFile(filePath, 'utf8');
    const jsonData = JSON.parse(fileContent);
    
    if (!jsonData.features || !Array.isArray(jsonData.features)) {
      console.warn(`No features array found in file: ${filePath}`);
      return;
    }
    
    const client = await pool.connect();
    try {
      // await client.query('BEGIN');
      
      // Insert file record first
      const fileName = path.basename(filePath);
      const fileResult = await client.query(
        `INSERT INTO genomes (filename) 
         VALUES ($1) 
         RETURNING id`,
        [
          fileName
        ]
      );
      
      const genomeId = fileResult.rows[0].id;
      
      // Insert all features from the file
      for (const feature of jsonData.features) {
        await insertFeature(feature, genomeId, client);
      }
      
      // await client.query('COMMIT');
      console.log(`Successfully imported ${jsonData.features.length} features from ${filePath}`);
    } catch (err) {
      // await client.query('ROLLBACK');
      console.error(`Error processing file ${filePath}:`, err);
    } finally {
      client.release();
    }
  } catch (err) {
    console.error(`Error reading or parsing file ${filePath}:`, err);
  }
}

/**
 * Process all JSON files in a directory
 * @param {string} dirPath - Path to directory containing JSON files
 */
async function processDirectory(dirPath) {
  try {
    const files = await fs.promises.readdir(dirPath);
    const jsonFiles = files.filter(file => file.toLowerCase().endsWith('.json'));
    
    console.log(`Found ${jsonFiles.length} JSON files to process`);
    
    let promises = [];
    for (const file of jsonFiles) {
      promises.push(
        processFile(path.join(dirPath, file))
      );
      if (promises.length >= 4) {
        await Promise.all(promises);
        promises = [];
      }
    }
    
    console.log('Finished processing all files');
  } catch (err) {
    console.error('Error reading directory:', err);
  } finally {
    // Close pool when done
    await pool.end();
  }
}

// Directory path containing JSON files to import
const directoryPath = process.argv[2] || './';

// Start processing
processDirectory(directoryPath);