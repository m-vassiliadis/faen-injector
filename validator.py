import json
from pathlib import Path
from typing import Any, Dict, Tuple

import rdflib
from pyshacl import validate

from console_utils import print_warning


class DatasetValidator:
    """
    Validator for CDE datasets using SHACL shapes.
    """

    def __init__(self):
        """
        Initialize the validator by loading SHACL shapes and schema context.
        """
        self.script_dir = Path(__file__).parent
        self.validation_dir = self.script_dir / "validation"
        self.shapes_path = self.validation_dir / "shapes.ttl"
        self.schema_path = self.validation_dir / "datacellar_schema.json"

        if not self.shapes_path.exists():
            print_warning(f"SHACL shapes file not found at {self.shapes_path}")

        if not self.schema_path.exists():
            print_warning(f"Schema context file not found at {self.schema_path}")

    def validate(self, dataset_definition: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validate a dataset definition against SHACL shapes.

        Args:
            dataset_definition: The dataset definition dictionary (JSON-LD).

        Returns:
            Tuple containing:
            - bool: True if validation passed, False otherwise.
            - str: Validation report or error message.
        """
        if not self.shapes_path.exists():
            return False, "SHACL shapes file missing, cannot validate."

        try:
            # Convert JSON-LD to RDF graph
            data_graph = rdflib.Graph()

            # We need to resolve the context locally or handle it.
            # For simplicity, we can dump to string and parse with json-ld format.
            # However, rdflib's json-ld parser might need internet access for remote contexts.
            # Since the context is usually inline or standard, it might work.
            # If datacellar schema is local, we might need to ensure it's resolvable.

            # A more robust approach for this specific setup where we have the schema locally:
            # We can try to parse it.

            json_data = json.dumps(dataset_definition)
            data_graph.parse(data=json_data, format="json-ld")

            # Load SHACL shapes
            shacl_graph = rdflib.Graph()
            shacl_graph.parse(str(self.shapes_path), format="turtle")

            # Run validation
            conforms, results_graph, results_text = validate(
                data_graph,
                shacl_graph=shacl_graph,
                inference="rdfs",
                abort_on_first=False,
                allow_warnings=False,
                meta_shacl=False,
                advanced=True,
                js=False,
                debug=False,
            )

            return conforms, results_text

        except Exception as e:
            return False, f"Validation process error: {str(e)}"
