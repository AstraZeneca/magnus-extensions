import logging
from string import Template as str_template
from typing import List

from magnus import defaults, integration, utils
from magnus.executor import BaseExecutor
from magnus.graph import Graph
from magnus.nodes import BaseNode

logger = logging.getLogger(defaults.NAME)


class SlurmJobExecutor(BaseExecutor):
    service_name = "slurm-job"

    class Config(BaseExecutor.Config):
        partition: str = "core"
        nodes: int = 1  # 1 node
        memory: str = "2G"  # 2 GB
        time: str = "0-00:15:00"  # 15 mins
        shell_file_name: str = "run_script.sh"
        # Mention any valid configuration of slurm, this over-rides the explicit values
        advanced_configurations: dict = {}
        # List of commands to execute after the SBATCH pragma
        implementation_instructions: List[str] = []

    def prepare_for_graph_execution(self):
        """
        This method would be called prior to calling execute_graph.
        Perform any steps required before doing the graph execution.

        The most common implementation is to prepare a run log for the run if the run uses local interactive compute.

        But in cases of actual rendering the job specs (eg: AWS step functions, K8's) we need not do anything.
        """

        integration.validate(self, self.run_log_store)
        integration.configure_for_traversal(self, self.run_log_store)

        integration.validate(self, self.catalog_handler)
        integration.configure_for_traversal(self, self.catalog_handler)

        integration.validate(self, self.secrets_handler)
        integration.configure_for_traversal(self, self.secrets_handler)

        integration.validate(self, self.experiment_tracker)
        integration.configure_for_traversal(self, self.experiment_tracker)

    def prepare_for_node_execution(self):
        """
        Perform any modifications to the services prior to execution of the node.

        Args:
            node (Node): [description]
            map_variable (dict, optional): [description]. Defaults to None.
        """

        integration.validate(self, self.run_log_store)
        integration.configure_for_execution(self, self.run_log_store)

        integration.validate(self, self.catalog_handler)
        integration.configure_for_execution(self, self.catalog_handler)

        integration.validate(self, self.secrets_handler)
        integration.configure_for_execution(self, self.secrets_handler)

        integration.validate(self, self.experiment_tracker)
        integration.configure_for_execution(self, self.experiment_tracker)

        self._set_up_run_log(exists_ok=True)

    def execute_job(self, node: BaseNode):
        command = utils.get_job_execution_command(self, node)
        logger.info(f"Rendering slurm instruction with : {command}")

        lines = ["#!/bin/bash -l"]
        lines.append(f"#SBATCH --partition {self.config.partition}")
        lines.append(f"#SBATCH -N {self.config.nodes}")
        lines.append(f"#SBATCH --mem={self.config.memory}")
        lines.append(f"#SBATCH --time={self.config.time}")

        for key, value in self.config.advanced_configurations.items():
            # Assumption that all keys are words
            lines.append(f"#SBATCH --{key}={value}")

        lines.append(f"#SBATCH --job-name={self.run_id}")
        lines.append("\n")

        for instruction in self.config.implementation_instructions:
            instruction = str_template(instruction).safe_substitute(run_id=self.run_id)
            lines.append(instruction)

        lines.append("\n")
        lines.append(command)

        with open(self.config.shell_file_name, "w") as fw:
            for line in lines:
                fw.write(line.strip() + "\n")

    def execute_node(self, node: BaseNode, map_variable: dict = None, **kwargs):
        step_log = self.run_log_store.create_step_log(
            node.name, node._get_step_log_name(map_variable)
        )

        self.add_code_identities(node=node, step_log=step_log)

        step_log.step_type = node.node_type
        step_log.status = defaults.PROCESSING
        self.run_log_store.add_step_log(step_log, self.run_id)

        super()._execute_node(node, map_variable=map_variable, **kwargs)

        step_log = self.run_log_store.get_step_log(
            node._get_step_log_name(map_variable), self.run_id
        )
        if step_log.status == defaults.FAIL:
            raise Exception(f"Step {node.name} failed")

    def execute_graph(self, dag: Graph, map_variable: dict = None, **kwargs):
        msg = "This executor is not supported to execute any graphs but only jobs (functions or notebooks)"
        raise NotImplementedError(msg)

    def send_return_code(self, stage="traversal"):
        """
        Convenience function used by pipeline to send return code to the caller of the cli

        Raises:
            Exception: If the pipeline execution failed
        """
        if (
            stage != "traversal"
        ):  # traversal does no actual execution, so return code is pointless
            run_id = self.run_id

            run_log = self.run_log_store.get_run_log_by_id(run_id=run_id, full=False)
            if run_log.status == defaults.FAIL:
                raise Exception("Pipeline execution failed")
