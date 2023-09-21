
from types import SimpleNamespace
from typing import Any, List
import copy

from domain.diagram_node_status_enum import DiagramNodeStatusEnum


class WorkflowDiagramService:

    def __init__(self) -> None:
        pass


    def __set_node_status(self, 
                          status : str, 
                          node : SimpleNamespace):
    
        node.status = status
        if status in [DiagramNodeStatusEnum.RUNNING, 
                      DiagramNodeStatusEnum.IN_PROGRESS]:
            node.active = True;
        if status == DiagramNodeStatusEnum.COMPLETED:
            node.active = False;
        if status == DiagramNodeStatusEnum.FAILED:
            node.active = True;
        else:
            node.active = False;


    def __update_group_status(self, 
                              nodes : List[SimpleNamespace],
                              group : SimpleNamespace,
                              status : DiagramNodeStatusEnum
                            ) -> None:
        parent_group = None
        group_id = getattr(group, "group", None)
        if group_id is not None:
            parent_group = next((node for node in nodes if node.id == group.group and node.isGroup), None)

        if status in [DiagramNodeStatusEnum.RUNNING, DiagramNodeStatusEnum.IN_PROGRESS]:
            group.status = DiagramNodeStatusEnum.IN_PROGRESS
            group.active = True

            if parent_group is not None:
                self.__update_group_status(
                    nodes = nodes, 
                    group = parent_group, 
                    status = DiagramNodeStatusEnum.IN_PROGRESS)

        elif status in [DiagramNodeStatusEnum.COMPLETED, DiagramNodeStatusEnum.SKIPPED]:
            group_tasks = [node for node in nodes \
                           if getattr(node, "group", None) == group.id and not node.isGroup]
            has_nodes_in_progress = len([t for t in group_tasks \
                                         if t.status in [DiagramNodeStatusEnum.IN_PROGRESS, 
                                                         DiagramNodeStatusEnum.RUNNING]]) > 0
            all_nodes_skipped = len(group_tasks) == len([t for t in group_tasks \
                                                         if t.status in [DiagramNodeStatusEnum.SKIPPED, 
                                                                         DiagramNodeStatusEnum.NOT_STARTED]])

            group.status = status
            group.active = False
            
            if has_nodes_in_progress:
                if parent_group is not None:
                    self.__update_group_status(
                        nodes = nodes, 
                        group = parent_group, 
                        status = DiagramNodeStatusEnum.IN_PROGRESS)

            elif all_nodes_skipped:
                if parent_group is not None:
                    self.__update_group_status(
                        nodes = nodes, 
                        group = parent_group, 
                        status = DiagramNodeStatusEnum.SKIPPED)


    def update_current_tasks_status(self,
                                    diagram : SimpleNamespace,
                                    task_name : str,
                                    status : DiagramNodeStatusEnum = DiagramNodeStatusEnum.IN_PROGRESS
                                   ) -> SimpleNamespace:
        if diagram is None: raise ValueError(f"diagram cannot be empty")
        if (task_name is None or len(str(task_name)) == 0): raise ValueError(f"task_name cannot be empty")
        if (status is None or len(str(status)) == 0): raise ValueError(f"task_status cannot be empty")

        diagram_updated = copy.deepcopy(diagram)
        node = next((node for node in diagram_updated.nodes \
            if node.name == task_name and node.category == 'task'), None)

        if node is not None:
            if node.status != DiagramNodeStatusEnum.FAILED:
                self.__set_node_status(status = status, node = node)

            group_id = getattr(node, "group", None)
            if group_id is not None:
                group = next((g for g in diagram_updated.nodes if g.id == group_id and g.isGroup), None)
                if (group is not None):
                    self.__update_group_status(diagram_updated.nodes, group, status)

        return diagram_updated
