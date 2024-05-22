import enum

from swarm.models.json_field import JSONField


class ProfileType(enum.Enum):
    BalancedProfile = enum.auto()
    CoreIntensiveProfile = enum.auto()
    RamIntensiveProfile = enum.auto()
    DiskIntensiveProfile = enum.auto()
    CustomProfile = enum.auto()

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name


class Profile(JSONField):
    def __init__(self, **kwargs):
        self.core_weight = 0
        self.ram_weight = 0
        self.disk_weight = 0
        self._set_fields(**kwargs)

    def _set_fields(self, forgiving=False, **kwargs):
        """
        Universal integer setter for all fields.
        Values should be non-negative integers. Throws a CapacityException
        if you try to set a non-existent field.
        :param kwargs:
        :return: self to support call chaining
        """
        for k, v in kwargs.items():
            if v is not None:
                assert v >= 0
                assert isinstance(v, float)
            try:
                # will toss an exception if field is not defined
                self.__getattribute__(k)
                self.__setattr__(k, v)
            except AttributeError:
                report = f"Unable to set field {k} of capacity, no such field available "\
                       f"{[k for k in self.__dict__.keys()]}"
                if forgiving:
                    print(report)
                else:
                    raise ProfileException(report)
        return self


class ProfileException(Exception):
    """
    Exception with a capacity
    """
    def __init__(self, msg: str):
        assert msg is not None
        super().__init__(f"Profile exception: {msg}")


PROFILE_MAP = {
    str(ProfileType.BalancedProfile): Profile(core_weight=0.3, ram_weight=0.3, disk_weight=0.4),
    str(ProfileType.CoreIntensiveProfile): Profile(core_weight=0.6, ram_weight=0.2, disk_weight=0.2),
    str(ProfileType.RamIntensiveProfile): Profile(core_weight=0.2, ram_weight=0.6, disk_weight=0.2)
}

