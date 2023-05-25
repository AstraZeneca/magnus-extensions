import json
import logging
import random
import shlex
import string
from collections import OrderedDict
from string import Template as str_template
from typing import Any, Dict, List, Optional

from magnus import defaults, integration, utils
from magnus.executor import BaseExecutor
from magnus.graph import Graph, create_node, search_node_by_internal_name
from magnus.nodes import BaseNode
from pydantic import BaseModel
from ruamel.yaml import YAML

logger = logging.getLogger(defaults.NAME)


class DagTemplate(BaseModel):
    # Contains the shell script of only the parent dag at this moment
    script_name: str = ""
    dag_execution_lines: List[str] = []
    environment_variables: Dict[str, str] = {}
    env_setup: str = ""

    def render(self):
        file_name = self.script_name

        with open(file_name, "w") as fw:
            fw.write("#!/bin/bash -l\n\n")

            # Set up the environment of magnus
            fw.write("# Set up the run time if not present\n")
            fw.write(self.env_setup)

            fw.write("# Generate the run_id\n")
            fw.write(
                'run_id=$(python -c "from magnus import utils; print(utils.generate_run_id())")\n'
            )

            fw.write('echo "Running $run_id"')

            fw.write("# Exposing magnus specific environmental variables, if any!\n")
            for key, value in self.environment_variables.items():
                fw.write(f'export MAGNUS_PRM_{key}="{value}"\n')

            fw.write("\n# Create a log folder\n")
            fw.write("mkdir -p slurm_logs/$run_id/\n")

            fw.write("\n# Dag specific order\n")
            fw.write("\n".join(self.dag_execution_lines))


class NodeRenderer:
    allowed_node_types: List[str] = []

    def __init__(self, executor: BaseExecutor, node: BaseNode) -> None:
        self.executor = executor
        self.node = node
        self.env_setup: str = ""

    def render(self, list_of_iter_values: List = None):
        pass


class ExecutionNode(NodeRenderer):
    allowed_node_types = ["task", "as-is", "success", "fail"]

    def render(self, list_of_iter_values: List = None):
        """
        Compose the map variable and create the execution command.
        Create an input to the command.
        create_container_template : creates an argument for the list of iter values
        """
        clean_name = self.executor.get_clean_name(self.node)
        map_variable = {}  # self.executor.compose_map_variable(list_of_iter_values)
        command = utils.get_node_execution_command(
            self.executor,
            self.node,
            over_write_run_id=self.executor.run_id_placeholder,
            map_variable=map_variable,
        )

        lines = ["#!/bin/bash -l"]
        lines.append(f"#SBATCH --partition {self.executor.config.partition}")
        lines.append(f"#SBATCH -N {self.executor.config.nodes}")
        lines.append(f"#SBATCH --mem={self.executor.config.memory}")
        lines.append(f"#SBATCH --time={self.executor.config.time}")

        mode_config = self.executor._resolve_executor_config(self.node)
        advanced_configurations = mode_config["advanced_configurations"]

        for key, value in advanced_configurations.items():
            # Assumption that all keys are words
            lines.append(f"#SBATCH --{key}={value}")

        lines.append(f"#SBATCH --job-name={clean_name}")

        lines.append("\n")

        # Capture the run_id from the input argument.
        # Adding this before the sbatch pragma will make sbatch ignore commands
        lines.append("run_id=$1\n")

        if mode_config["env_setup"] != self.executor.config.env_setup:
            self.env_setup = mode_config["env_setup"]

        # Do the env set up if required
        instruction = str_template(self.env_setup).safe_substitute(
            run_id=self.executor.run_id_placeholder
        )
        lines.append(f"{instruction}\n")

        # Do the env activation
        activation = mode_config["env_activation"]
        activation = str_template(activation).safe_substitute(
            run_id=self.executor.run_id_placeholder
        )

        lines.append(f"{activation}\n")

        # Finally do the command
        lines.append(command)

        with open(clean_name, "w") as fw:
            for line in lines:
                fw.write(line.strip() + "\n")


class DagNode(NodeRenderer):
    allowed_node_types = ["dag"]

    def render(self, list_of_iter_values: List = None):
        ...


class ParallelNode(NodeRenderer):
    allowed_node_types = ["parallel"]

    def render(self, list_of_iter_values: List = None):
        ...


class MapNode(NodeRenderer):
    allowed_node_types = ["map"]

    def render(self, list_of_iter_values: List = None):
        ...


def get_renderer(node):
    renderers = NodeRenderer.__subclasses__()

    for renderer in renderers:
        if node.node_type in renderer.allowed_node_types:
            return renderer
    raise Exception("This node type is not render-able")


class SlurmExecutor(BaseExecutor):
    service_name = "slurm"
    # All SBATCH jobs would be called SBATCH script <run_id>
    run_id_placeholder = "$run_id"

    class Config(BaseExecutor.Config):
        partition: str = "core"
        nodes: int = 1  # 1 node
        memory: str = "2G"  # 2 GB
        time: str = "0-00:15:00"  # 15 mins
        driver_script: str = "pipeline.sh"
        # Mention any valid configuration of slurm, this over-rides the explicit values
        advanced_configurations: dict = {}
        # List of commands to execute after the SBATCH pragma
        env_setup: str = ""
        env_activation: str

    def __init__(self, config: dict = None):
        super().__init__(config)

        self.dag_templates: List[DagTemplate] = []
        self.clean_names: dict = {}

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

    def execute_node(self, node: BaseNode, map_variable: dict = None, **kwargs):
        step_log = self.run_log_store.create_step_log(
            node.name, node._get_step_log_name(map_variable)
        )

        self.add_code_identities(node=node, step_log=step_log)

        step_log.step_type = node.node_type
        step_log.status = defaults.PROCESSING
        self.run_log_store.add_step_log(step_log, self.run_id)

        super()._execute_node(node, map_variable=map_variable, **kwargs)

        # Implicit fail
        if self.dag:
            # functions and notebooks do not have dags
            _, current_branch = search_node_by_internal_name(
                dag=self.dag, internal_name=node.internal_name
            )
            _, next_node_name = self._get_status_and_next_node_name(
                node, current_branch, map_variable=map_variable
            )
            if next_node_name:
                # Terminal nodes do not have next node name
                next_node = current_branch.get_node_by_name(next_node_name)

                if next_node.node_type == defaults.FAIL:
                    self.execute_node(next_node, map_variable=map_variable)

        step_log = self.run_log_store.get_step_log(
            node._get_step_log_name(map_variable), self.run_id
        )
        if step_log.status == defaults.FAIL:
            raise Exception(f"Step {node.name} failed")

    def fan_out(self, node: BaseNode, map_variable: dict):
        super().fan_out(node, map_variable)

        ...

    def fan_in(self, node: BaseNode, map_variable: dict):
        super().fan_in(node, map_variable)

    def sanitize_name(self, name):
        return name.replace(" ", "_").replace(".", "_")

    def get_clean_name(self, node: BaseNode):
        # Cache names for the node
        if node.internal_name not in self.clean_names:
            sanitized = self.sanitize_name(node.name)
            # tag = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
            self.clean_names[
                node.internal_name
            ] = f"{sanitized}_{node.node_type}"  # -{tag}"

        return self.clean_names[node.internal_name]

    def get_parameters(self):
        parameters = utils.get_user_set_parameters()
        if self.parameters_file:
            parameters.update(utils.load_yaml(self.parameters_file))
        return parameters

    def compose_map_variable(
        self, list_of_iter_values: Optional[List] = None
    ) -> OrderedDict:
        ...

    def _create_fan_out_template(
        self, composite_node, list_of_iter_values: List = None
    ):
        ...

    def _create_fan_in_template(self, composite_node, list_of_iter_values: List = None):
        ...

    def _gather_task_templates_of_dag(
        self, dag: Graph, script_name: str = "", list_of_iter_values: List = None
    ):
        current_node = dag.start_at
        previous_node = None
        previous_node_template_name = None

        dag_template = DagTemplate(script_name=script_name)

        if not dag.internal_branch_name:
            # We are in the parent branch, add the parameters and the env setup
            for key, value in self.get_parameters().items():
                # Get the value from work flow parameters for dynamic behavior
                if isinstance(value, dict) or isinstance(value, list):
                    continue
                dag_template.environment_variables[key] = value

            dag_template.env_setup = self.config.env_setup

        lines = []
        sbatch_start = (
            f"sbatch --parsable --output=slurm_logs/{self.run_id_placeholder}/job-%x-%j.out "
            f" --error=slurm_logs/{self.run_id_placeholder}/job-%x-%j.out"
        )
        while True:
            working_on = dag.get_node_by_name(current_node)
            if previous_node == current_node:
                raise Exception("Potentially running in a infinite loop")

            render_obj = get_renderer(working_on)(executor=self, node=working_on)
            render_obj.render(list_of_iter_values=list_of_iter_values.copy())

            clean_name = self.get_clean_name(working_on)

            # Link the current node to previous node, if the previous node was successful.
            if previous_node:
                # Add the line to the dag template about this dependency.
                lines.append(
                    f"{clean_name}_id=$({sbatch_start} --dependency=afterok:${previous_node_template_name}_id"
                    f" {clean_name} {self.run_id_placeholder})\n"
                )
                # TODO: Restarts can be added by submitting the job multiple times and separating by ?
            else:
                lines.append(
                    f"{clean_name}_id=$({sbatch_start} {clean_name} {self.run_id_placeholder})\n"
                )

            # On failure nodes
            if working_on._get_on_failure_node():
                failure_node = dag.get_node_by_name(working_on._get_on_failure_node())

                failure_template_name = self.get_clean_name(failure_node)
                # Add the line to the dag template about this dependency.
                lines.append(
                    f"{clean_name}_id=$(sbatch_start --dependency=afternotok:${failure_template_name}_id"
                    f" {clean_name} {self.run_id_placeholder})\n"
                )

                # TODO: Restarts can be added by submitting the job multiple times and separating by ?

            # If we are in a map node, we need to add the values as arguments

            # Move ahead to the next node
            previous_node = current_node
            previous_node_template_name = self.get_clean_name(working_on)

            if working_on.node_type in ["success", "fail"]:
                break

            current_node = working_on._get_next_node()

        dag_template.dag_execution_lines = lines

        # Add the dag template to the list of templates
        self.dag_templates.append(dag_template)

    def execute_graph(self, dag: Graph, map_variable: dict = None, **kwargs):
        # Container specifications are globally collected and added at the end.
        # Dag specifications are added as part of the dag traversal.
        self._gather_task_templates_of_dag(
            dag=dag,
            list_of_iter_values=[],
            script_name=self.config.driver_script,
        )

        for template in self.dag_templates:
            template.render()

    def execute_job(self, node: BaseNode):
        """
        This is not the best use of this executor but it works!!
        """
        raise NotImplementedError()

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
