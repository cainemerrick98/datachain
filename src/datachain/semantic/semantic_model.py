from .models import Metric, Dimension, Filter

class SemanticModel():
    def __init__(self):
        self.metrics: dict[str, Metric] = {}
        self.dimensions: dict[str, Dimension] = {}
        self.filters: dict[str, Filter] = {}

    def register_metric(self, metric: Metric):
        self.metrics[metric.name] = metric

    def register_dimension(self, dimension: Dimension):
        self.dimensions[dimension.name] = dimension

    def register_filter(self, filter: Filter):
        self.filters[filter.name] = filter

    def get_metric(self, name: str) -> Metric | None:
        return self.metrics.get(name)
    
    def get_dimension(self, name: str) -> Dimension | None: 
        return self.dimensions.get(name)
    
    def get_filter(self, name: str) -> Filter | None:
        return self.filters.get(name)
    
semantic_model = SemanticModel()