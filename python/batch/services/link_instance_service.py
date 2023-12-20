from domain.cirrus_object import CirrusObject
from domain.identifier import Identifier

from repositories.cirrus_object_repository import CirrusObjectRepository
from repositories.link_type_repository import LinkTypeRepository


class LinkInstanceService:
    
    _link_type_repository : LinkTypeRepository = None
    

    def __init__(self,
                 link_type_repository : LinkTypeRepository
                ) -> None:
        if (link_type_repository is None): raise ValueError(f"link_instance_service cannot be empty")

        self._link_type_repository = link_type_repository


    def get_linked_object(self, 
                          cirrus_object : CirrusObject, 
                          link_type_identifier : Identifier,
                          repository : CirrusObjectRepository,
                          link_obj_attr_name : str = "businessObject2"
                         ) -> CirrusObject:
        object_links = cirrus_object.get_object_links()
        link_type = self._link_type_repository.get_by_identifier(link_type_identifier)

        for _, link in enumerate(object_links):
            if link.linkType == link_type.key:
                object_key = getattr(link, link_obj_attr_name, None)
                if object_key is not None:
                    object_instance, _ = repository.get_by_key(
                            key = object_key,
                        )
                    return object_instance
        return None