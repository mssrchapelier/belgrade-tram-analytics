from typing import Type, List
from pathlib import Path

from pydantic import BaseModel
import erdantic as erd
from erdantic.core import EntityRelationshipDiagram

def draw_diagram(model_types: List[Type[BaseModel]],
                 terminal_models: List[Type[BaseModel]],
                 out_path: str | Path) -> None:
    diagram: EntityRelationshipDiagram = erd.create(
        *model_types,
        terminal_models=terminal_models
    )
    diagram.draw(out_path,
                 # do not render the default label
                 graph_attr={"label": ""})
    print(f"Diagram saved to: {out_path}")

