-- Add a files table to track imported files
CREATE TABLE genomes (
  id SERIAL PRIMARY KEY,
  filename VARCHAR(255) NOT NULL,
  filepath TEXT NOT NULL,
  import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  file_metadata JSONB -- Optional: store additional file metadata like genome info
);

CREATE TABLE features (
  id SERIAL PRIMARY KEY,
  genomes_id INTEGER REFERENCES genomes(id), -- Reference to the source file
  feature_id VARCHAR(255),
  type VARCHAR(50),
  contig VARCHAR(255),
  start INTEGER,
  stop INTEGER,
  strand VARCHAR(1),
  frame INTEGER,
  gene VARCHAR(255),
  product TEXT,
  nt TEXT,
  aa TEXT,
  aa_hexdigest VARCHAR(32),
  start_type VARCHAR(10),
  rbs_motif VARCHAR(255),
  locus VARCHAR(255)
);

CREATE TABLE db_xrefs (
  id SERIAL PRIMARY KEY,
  feature_id INTEGER REFERENCES features(id),
  db_xref VARCHAR(255)
);

CREATE TABLE genes (
  id SERIAL PRIMARY KEY,
  feature_id INTEGER REFERENCES features(id),
  gene VARCHAR(255)
);

CREATE TABLE ups (
  id SERIAL PRIMARY KEY,
  feature_id INTEGER REFERENCES features(id),
  uniparc_id VARCHAR(255),
  ncbi_nrp_id VARCHAR(255),
  uniref100_id VARCHAR(255)
);

CREATE TABLE ups_db_xrefs (
  id SERIAL PRIMARY KEY,
  ups_id INTEGER REFERENCES ups(id),
  db_xref VARCHAR(255)
);

CREATE TABLE ips (
  id SERIAL PRIMARY KEY,
  feature_id INTEGER REFERENCES features(id),
  uniref100_id VARCHAR(255),
  uniref90_id VARCHAR(255)
);

CREATE TABLE psc (
  id SERIAL PRIMARY KEY,
  feature_id INTEGER REFERENCES features(id),
  uniref90_id VARCHAR(255),
  gene VARCHAR(255),
  product TEXT,
  uniref50_id VARCHAR(255),
  cog_id VARCHAR(50),
  cog_category VARCHAR(10)
);

CREATE TABLE psc_go_ids (
  id SERIAL PRIMARY KEY,
  psc_id INTEGER REFERENCES psc(id),
  go_id VARCHAR(50)
);

CREATE TABLE psc_ec_ids (
  id SERIAL PRIMARY KEY,
  psc_id INTEGER REFERENCES psc(id),
  ec_id VARCHAR(50)
);

CREATE TABLE pscc (
  id SERIAL PRIMARY KEY,
  feature_id INTEGER REFERENCES features(id),
  uniref50_id VARCHAR(255),
  product TEXT
);

CREATE TABLE pscc_db_xrefs (
  id SERIAL PRIMARY KEY,
  pscc_id INTEGER REFERENCES pscc(id),
  db_xref VARCHAR(255)
);

CREATE INDEX idx_features_feature_id ON features(feature_id);
CREATE INDEX idx_features_gene ON features(gene);
CREATE INDEX idx_features_contig ON features(contig);
CREATE INDEX idx_features_file_id ON features(file_id);
CREATE INDEX idx_files_filename ON files(filename);