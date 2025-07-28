from django.core.management.base import BaseCommand
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
        self.stdout.write(self.style.SUCCESS(f"Knowledge graph saved to {output_path}"))
