COMMAND_MODULES = {
    "catalog": "research_pipeline.cli.commands.catalog",
    "package": "research_pipeline.application.research_package",
    "run": "research_pipeline.application.research_run",
    "resume": "research_pipeline.application.runtime_lifecycle",
    "retry-node": "research_pipeline.application.runtime_lifecycle",
    "inspect": "research_pipeline.application.runtime_lifecycle",
    "rerun-from": "research_pipeline.application.runtime_lifecycle",
    "report": "research_pipeline.application.result_delivery",
    "compare": "research_pipeline.application.result_delivery",
    "export-result": "research_pipeline.application.result_delivery",
    "doctor": "research_pipeline.cli.commands.operations_lifecycle",
    "gc": "research_pipeline.cli.commands.operations_lifecycle",
    "capabilities": "research_pipeline.cli.commands.machine_discovery",
    "operator": "research_pipeline.cli.commands.machine_discovery",
    "artifact": "research_pipeline.cli.commands.machine_discovery",
    "workspace": "research_pipeline.cli.commands.workspace",
}

__all__ = ["COMMAND_MODULES"]
