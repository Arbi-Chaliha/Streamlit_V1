'''from django.core.management.base import BaseCommand
from troubleshooting.kg_utils import create_rdf_graph_from_xlsx

class Command(BaseCommand):
    help = 'Build or update the knowledge graph from an Excel file.'

    def add_arguments(self, parser):
        parser.add_argument('--xlsx', type=str, required=True, help='Path to the Excel file')
        parser.add_argument('--output', type=str, default='output_ORA_FNFM_KG.ttl', help='Path to save the generated TTL file')

    def handle(self, *args, **options):
        xlsx_path = options['xlsx']
        output_path = options['output']
        self.stdout.write(f"Building knowledge graph from {xlsx_path} ...")
        create_rdf_graph_from_xlsx(xlsx_path, output_path)
        self.stdout.write(self.style.SUCCESS(f"Knowledge graph saved to {output_path}"))'''
from django.core.management.base import BaseCommand, CommandError
from troubleshooting.kg_utils import create_rdf_graph_from_xlsx
import os
import pandas as pd

class Command(BaseCommand):
    help = 'Build or update the knowledge graph from an Excel file containing troubleshooting data.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--xlsx', 
            type=str, 
            required=True, 
            help='Path to the Excel file containing troubleshooting data'
        )
        parser.add_argument(
            '--output', 
            type=str, 
            default='output_ORA_FNFM_KG.ttl', 
            help='Path to save the generated TTL file (default: output_ORA_FNFM_KG.ttl)'
        )
        parser.add_argument(
            '--validate',
            action='store_true',
            help='Validate the Excel file structure before processing'
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Enable verbose output'
        )

    def handle(self, *args, **options):
        xlsx_path = options['xlsx']
        output_path = options['output']
        validate = options['validate']
        verbose = options['verbose']
        
        # Check if Excel file exists
        if not os.path.exists(xlsx_path):
            raise CommandError(f"Excel file not found: {xlsx_path}")
        
        # Validate Excel file structure if requested
        if validate:
            if not self.validate_excel_structure(xlsx_path, verbose):
                raise CommandError("Excel file validation failed")
        
        self.stdout.write(f"Building knowledge graph from {xlsx_path}...")
        
        try:
            # Create the knowledge graph
            result_path = create_rdf_graph_from_xlsx(xlsx_path, output_path)
            
            # Verify the output file was created
            if os.path.exists(result_path):
                file_size = os.path.getsize(result_path)
                self.stdout.write(
                    self.style.SUCCESS(
                        f"✓ Knowledge graph successfully saved to {result_path} "
                        f"({file_size:,} bytes)"
                    )
                )
                
                if verbose:
                    self.display_kg_stats(result_path)
            else:
                raise CommandError("Output file was not created")
                
        except Exception as e:
            raise CommandError(f"Error building knowledge graph: {str(e)}")

    def validate_excel_structure(self, xlsx_path, verbose=False):
        """Validate the structure of the Excel file"""
        try:
            # Read the Excel file with the same parameters as the main function
            df = pd.read_excel(xlsx_path, header=1, usecols=range(1, 7))
            
            if verbose:
                self.stdout.write(f"Excel file validation:")
                self.stdout.write(f"  - Rows: {len(df)}")
                self.stdout.write(f"  - Columns: {len(df.columns)}")
                self.stdout.write(f"  - Column names: {list(df.columns)}")
            
            # Check if we have the expected 6 columns
            if len(df.columns) != 6:
                self.stdout.write(
                    self.style.ERROR(
                        f"Expected 6 columns, found {len(df.columns)}"
                    )
                )
                return False
            
            # Check for empty dataframe
            if len(df) == 0:
                self.stdout.write(
                    self.style.ERROR("Excel file contains no data rows")
                )
                return False
            
            # Check for excessive null values
            null_percentage = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
            if null_percentage > 50:
                self.stdout.write(
                    self.style.WARNING(
                        f"High percentage of null values: {null_percentage:.1f}%"
                    )
                )
            
            # Display sample data if verbose
            if verbose:
                self.stdout.write("Sample data (first 3 rows):")
                for i, row in df.head(3).iterrows():
                    self.stdout.write(f"  Row {i}: {list(row.values)}")
            
            self.stdout.write(self.style.SUCCESS("✓ Excel file structure validation passed"))
            return True
            
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"Excel file validation failed: {str(e)}")
            )
            return False

    def display_kg_stats(self, kg_path):
        """Display statistics about the generated knowledge graph"""
        try:
            from rdflib import Graph
            
            g = Graph()
            g.parse(kg_path, format='turtle')
            
            # Count triples by type
            stats = {
                'total_triples': len(g),
                'subjects': len(set(g.subjects())),
                'predicates': len(set(g.predicates())),
                'objects': len(set(g.objects())),
            }
            
            # Count entities by type
            entity_types = {}
            type_query = """
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX troubleshooting_ora_fnfm_ontology_: <http://www.slb.com/ontologies/Troubleshooting_ORA_FNFM_Ontology_#>
            
            SELECT ?type (COUNT(?entity) as ?count)
            WHERE {
                ?entity rdf:type ?type .
                FILTER(?type IN (
                    troubleshooting_ora_fnfm_ontology_:Failure,
                    troubleshooting_ora_fnfm_ontology_:RootCause,
                    troubleshooting_ora_fnfm_ontology_:Trigger,
                    troubleshooting_ora_fnfm_ontology_:DataChannel
                ))
            }
            GROUP BY ?type
            """
            
            for row in g.query(type_query):
                type_name = str(row[0]).split('#')[-1]
                entity_types[type_name] = int(row[1])
            
            self.stdout.write("\nKnowledge Graph Statistics:")
            self.stdout.write(f"  - Total triples: {stats['total_triples']:,}")
            self.stdout.write(f"  - Unique subjects: {stats['subjects']:,}")
            self.stdout.write(f"  - Unique predicates: {stats['predicates']:,}")
            self.stdout.write(f"  - Unique objects: {stats['objects']:,}")
            
            if entity_types:
                self.stdout.write("\nEntity counts by type:")
                for entity_type, count in entity_types.items():
                    self.stdout.write(f"  - {entity_type}: {count}")
            
        except Exception as e:
            self.stdout.write(
                self.style.WARNING(f"Could not generate KG statistics: {str(e)}")
            )
