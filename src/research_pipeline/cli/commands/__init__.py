COMMAND_MODULES = {
    "catalog": "research_pipeline.cli.commands.catalog",
    "package": "research_pipeline.cli.commands.research_package",
    "run": "research_pipeline.cli.commands.research_run",
    "resume": "research_pipeline.cli.commands.runtime_lifecycle",
    "retry-node": "research_pipeline.cli.commands.runtime_lifecycle",
    "inspect": "research_pipeline.cli.commands.runtime_lifecycle",
    "rerun-from": "research_pipeline.cli.commands.runtime_lifecycle",
    "verify": "research_pipeline.cli.commands.evidence_lifecycle",
    "report": "research_pipeline.cli.commands.evidence_lifecycle",
    "compare": "research_pipeline.cli.commands.evidence_lifecycle",
    "export-result": "research_pipeline.cli.commands.evidence_lifecycle",
    "doctor": "research_pipeline.cli.commands.operations_lifecycle",
    "gc": "research_pipeline.cli.commands.operations_lifecycle",
    "capabilities": "research_pipeline.cli.commands.capabilities",
    "operator": "research_pipeline.cli.commands.machine_discovery",
    "artifact": "research_pipeline.cli.commands.machine_discovery",
    "recipe": "research_pipeline.cli.commands.machine_discovery",
    "workspace": "research_pipeline.cli.commands.workspace",
}

__all__ = ["COMMAND_MODULES"]
