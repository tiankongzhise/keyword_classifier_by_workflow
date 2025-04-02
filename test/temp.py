from typing import overload, Literal, Dict, List, Tuple, Union, cast
from src.kw_cf.models import ClassifiedResult, ClassifiedKeyword, UnMatchedKeyword, UnclassifiedKeywords

class YourClass:
    @overload
    @staticmethod
    def get_classification_groups(
        v: ClassifiedResult,
        mode: Literal["output_name"],
        keyword_status: Literal["match"]
    ) -> Dict[str, List[ClassifiedKeyword]]: ...

    @overload
    @staticmethod
    def get_classification_groups(
        v: ClassifiedResult,
        mode: Literal["output_name"],
        keyword_status: Literal["unmatch"]
    ) -> Dict[str, List[UnMatchedKeyword]]: ...

    @overload
    @staticmethod
    def get_classification_groups(
        v: ClassifiedResult,
        mode: Literal["sheet"],
        keyword_status: Literal["match"]
    ) -> Dict[Tuple[str, str], List[ClassifiedKeyword]]: ...

    @overload
    @staticmethod
    def get_classification_groups(
        v: ClassifiedResult,
        mode: Literal["sheet"],
        keyword_status: Literal["unmatch"]
    ) -> Dict[Tuple[str, str], List[UnMatchedKeyword]]: ...

    @overload
    @staticmethod
    def get_classification_groups(
        v: ClassifiedResult,
        mode: Literal["parent_rule"],
        keyword_status: Literal["match"]
    ) -> Dict[Tuple[str, str, str], List[ClassifiedKeyword]]: ...

    @overload
    @staticmethod
    def get_classification_groups(
        v: ClassifiedResult,
        mode: Literal["parent_rule"],
        keyword_status: Literal["unmatch"]
    ) -> Dict[Tuple[str, str, str], List[UnMatchedKeyword]]: ...

    @staticmethod
    def get_classification_groups(
        v: ClassifiedResult,
        mode: Literal["output_name", "sheet", "parent_rule"],
        keyword_status: Literal["match", "unmatch"]
    ) -> Union[
        Dict[str, List[ClassifiedKeyword]],
        Dict[str, List[UnMatchedKeyword]],
        Dict[Tuple[str, str], List[ClassifiedKeyword]],
        Dict[Tuple[str, str], List[UnMatchedKeyword]],
        Dict[Tuple[str, str, str], List[ClassifiedKeyword]],
        Dict[Tuple[str, str, str], List[UnMatchedKeyword]],
    ]:
        if mode == "output_name":
            if keyword_status == "match":
                return cast(Dict[str, List[ClassifiedKeyword]], v.get_grouped_keywords(mode, keyword_status))
            else:
                return cast(Dict[str, List[UnMatchedKeyword]], v.get_grouped_keywords(mode, keyword_status))
        elif mode == "sheet":
            if keyword_status == "match":
                return cast(Dict[Tuple[str, str], List[ClassifiedKeyword]], v.get_grouped_keywords(mode, keyword_status))
            else:
                return cast(Dict[Tuple[str, str], List[UnMatchedKeyword]], v.get_grouped_keywords(mode, keyword_status))
        elif mode == "parent_rule":
            if keyword_status == "match":
                return cast(Dict[Tuple[str, str, str], List[ClassifiedKeyword]], v.get_grouped_keywords(mode, keyword_status))
            else:
                return cast(Dict[Tuple[str, str, str], List[UnMatchedKeyword]], v.get_grouped_keywords(mode, keyword_status))
        else:
            raise ValueError(f"Invalid mode: {mode}")
