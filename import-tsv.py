#!/usr/bin/env python3
import os
import sys
import glob
import psycopg2
import argparse
from datetime import datetime


def parse_metadata(lines):
    """Extract metadata from header lines"""
    metadata = {
        'software_version': '',
        'database_version': '',
        'database_type': '',
        'doi': '',
        'url': ''
    }
    
    for line in lines:
        if line.startswith('# Software:'):
            metadata['software_version'] = line.replace('# Software:', '').strip()
        elif line.startswith('# Database:'):
            parts = line.replace('# Database:', '').strip().split(',')
            if len(parts) >= 1:
                metadata['database_version'] = parts[0].strip()
            if len(parts) >= 2:
                metadata['database_type'] = parts[1].strip()
        elif line.startswith('# DOI:'):
            metadata['doi'] = line.replace('# DOI:', '').strip()
        elif line.startswith('# URL:'):
            metadata['url'] = line.replace('# URL:', '').strip()
    
    return metadata


def parse_tsv_file(file_path):
    """Parse a Bakta TSV file and return metadata and annotations"""
    with open(file_path, 'r') as f:
        lines = f.readlines()
    
    # Separate header and data lines
    header_lines = [line for line in lines if line.startswith('#')]
    data_lines = [line for line in lines if not line.startswith('#')]
    
    # Extract sample_id from filename (without extension)
    sample_id = os.path.splitext(os.path.basename(file_path))[0]
    
    # Parse metadata from header
    metadata = parse_metadata(header_lines)
    metadata['sample_id'] = sample_id
    metadata['file_path'] = file_path
    
    # Parse annotations
    annotations = []
    for line in data_lines:
        if not line.strip():
            continue
            
        parts = line.strip().split('\t')
        if len(parts) < 8:  # Ensure we have at least the main fields
            continue
            
        annotation = {
            'sequence_id': parts[0],
            'feature_type': parts[1],
            'start_position': int(parts[2]),
            'stop_position': int(parts[3]),
            'strand': parts[4],
            'locus_tag': parts[5] if parts[5] else None,
            'gene': parts[6] if parts[6] else None,
            'product': parts[7] if parts[7] else None,
            'dbxrefs': parts[8].split(', ') if len(parts) > 8 and parts[8] else []
        }
        annotations.append(annotation)
    
    return metadata, annotations


def import_to_database(conn, metadata, annotations):
    """Import metadata and annotations to the PostgreSQL database"""
    cursor = conn.cursor()
    
    try:
        # Start transaction
        cursor.execute("BEGIN")
        
        # Insert genome data and get its ID
        cursor.execute("""
            INSERT INTO genomes (sample_id, software_version, database_version, database_type, doi, url, file_path)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            metadata['sample_id'],
            metadata['software_version'],
            metadata['database_version'],
            metadata['database_type'],
            metadata['doi'],
            metadata['url'],
            metadata['file_path']
        ))
        
        genome_id = cursor.fetchone()[0]
        
        # Insert annotations in batches for efficiency
        batch_size = 1000
        for i in range(0, len(annotations), batch_size):
            batch = annotations[i:i+batch_size]
            
            values_str = []
            values_list = []
            
            for annot in batch:
                values_str.append("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                values_list.extend([
                    genome_id,
                    annot['sequence_id'],
                    annot['feature_type'],
                    annot['start_position'],
                    annot['stop_position'],
                    annot['strand'],
                    annot['locus_tag'],
                    annot['gene'],
                    annot['product'],
                    annot['dbxrefs'] if annot['dbxrefs'] else None
                ])
            
            query = f"""
                INSERT INTO annotations 
                (genome_id, sequence_id, feature_type, start_position, stop_position, 
                strand, locus_tag, gene, product, dbxrefs)
                VALUES {', '.join(values_str)}
            """
            
            cursor.execute(query, values_list)
        
        # Commit the transaction
        cursor.execute("COMMIT")
        
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(f"Error during database import: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(description='Import Bakta annotation TSV files into PostgreSQL database')
    parser.add_argument('--folder', help='Folder containing Bakta TSV files')
    parser.add_argument('--db-name', default='bakta_annotations', help='PostgreSQL database name')
    parser.add_argument('--db-user', default='postgres', help='PostgreSQL username')
    parser.add_argument('--db-password', help='PostgreSQL password')
    parser.add_argument('--db-host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--db-port', default='5432', help='PostgreSQL port')
    parser.add_argument('--pattern', default='*.tsv', help='File pattern to match TSV files')
    
    args = parser.parse_args()
    
    # Connect to PostgreSQL database
    try:
        conn = psycopg2.connect(
            dbname=args.db_name,
            user=args.db_user,
            password=args.db_password,
            host=args.db_host,
            port=args.db_port
        )
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)
    
    # Find all TSV files in the specified folder
    folder_path = os.path.abspath(args.folder)
    file_pattern = os.path.join(folder_path, args.pattern)
    tsv_files = glob.glob(file_pattern)
    
    if not tsv_files:
        print(f"No TSV files found in {folder_path} matching pattern {args.pattern}")
        sys.exit(1)
    
    print(f"Found {len(tsv_files)} TSV files to import")
    
    # Process each file
    success_count = 0
    error_count = 0
    
    for i, file_path in enumerate(tsv_files, 1):
        try:
            print(f"[{i}/{len(tsv_files)}] Processing {os.path.basename(file_path)}")
            metadata, annotations = parse_tsv_file(file_path)
            import_to_database(conn, metadata, annotations)
            print(f"  Imported {len(annotations)} annotations")
            success_count += 1
        except Exception as e:
            print(f"  Error processing {file_path}: {e}")
            error_count += 1
    
    conn.close()
    print(f"Import completed. Successfully processed {success_count} files. Failed: {error_count} files.")


if __name__ == "__main__":
    main()