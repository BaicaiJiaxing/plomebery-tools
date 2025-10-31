from enum import Enum

class CompanyNameEnum(Enum):
    DS_SJS = '石景山'
    DS_DX = '大兴'
    DS_MTG = '门头沟'
    DS_HR = '怀柔'
    DS_JY = '缙阳'
    DS_MY = '檀州'
    DS_LQ = '良泉'
    DS_TZ = '通州'
    DS_QB = '清北'
    DS_HD = '海淀'
    DS_CXD = '长辛店'
    DS_CY = '朝阳'
    DS_SQ = '市区'
    DS_FT = '丰台'
    DS_JC = '稽查'

    @classmethod
    def get_name(cls, company_code: str) -> str:
        """通过英文标识获取中文名，不区分大小写"""
        if not company_code:
            return ""

        upper_code = company_code.upper()

        for member_name, member in cls.__members__.items():
            if member_name == upper_code:
                return member.value

        return company_code