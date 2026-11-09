"""
Pipeline configuration.
"""
import yaml


class PipelineConfig:
    """Represents a pipeline configuration."""
    def __init__(self, raw: dict):
        self.name = raw['pipeline']['name']
        self.source = raw['source']
        self.transformation_steps = raw['transformations']
        self.output_columns = raw.get('output')
        self.validation_rules = raw.get('validation')


    @classmethod
    def from_yaml(cls, path: str) -> "PipelineConfig":
        """Load configuration from a YAML file."""
        # raise NotImplementedError
        with open(path,"r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        return cls(raw)
