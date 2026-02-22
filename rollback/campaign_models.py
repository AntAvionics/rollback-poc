from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class Campaign:
    id: int
    flight_impression_cap: Optional[int] = None  
    client_impression_cap: Optional[int] = None
    is_active: Optional[bool] = None
    expired_at: Optional[datetime] = None
    targeting_language: Optional[List[str]] = None
    targeting_zones: Dict[str, bool] = field(default_factory=dict)
    

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"id": self.id}
        if self.flight_impression_cap is not None:
            d["flightImpressionCap"] = self.flight_impression_cap
        if self.client_impression_cap is not None:
            d["clientImpressionCap"] = self.client_impression_cap
        if self.is_active is not None:
            d["is_active"] = self.is_active
        if self.expired_at is not None:
            d["expired_at"] = self.expired_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        if self.targeting_language is not None:
            d["targeting_language"] = self.targeting_language
        if self.targeting_zones:
            d["targeting_zones"] = self.targeting_zones
        return d

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Campaign":
        expired_raw = data.get("expired_at")
        expired_at: Optional[datetime] = None
        if isinstance(expired_raw, str):
            expired_at = datetime.fromisoformat(expired_raw.rstrip("Z"))

        return Campaign(
            id=data["id"],
            flight_impression_cap=data.get("flightImpressionCap"),
            client_impression_cap=data.get("clientImpressionCap"),
            is_active=data.get("is_active"),
            expired_at=expired_at,
            targeting_language=data.get("targeting_language"),
            targeting_zones=data.get("targeting_zones", {})
        )


@dataclass
class Creative:
    key: str              # "{ad_group_id}-{creative_id}"
    id: int               
    start_date: str       
    end_date: str         

    def to_dict(self) -> Dict[str, Any]:
        return {
            self.key: {
                "id": self.id,
                "startDate": self.start_date,
                "endDate": self.end_date,
            }
        }

    @staticmethod
    def from_dict(key: str, data: Dict[str, Any]) -> "Creative":
        return Creative(
            key=key,
            id=data["id"],
            start_date=data["startDate"],
            end_date=data["endDate"],
        )


@dataclass
class CampaignUpdate:
    adload_version: str
    campaigns: List[Campaign] = field(default_factory=list)
    creatives: List[Creative] = field(default_factory=list)
    targeting_zones: Dict[str, bool] = field(default_factory=dict)
    tail_number: Optional[str] = None   # set when airside includes it in request

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "adload_version": self.adload_version,
            "campaigns": [c.to_dict() for c in self.campaigns],
            "creatives": [c.to_dict() for c in self.creatives],
        }
        if self.tail_number is not None:
            d["tail_number"] = self.tail_number
        if self.targeting_zones:
            d["targeting_zones"] = self.targeting_zones
        return d
    
    @staticmethod
    def from_dict(
        data: Dict[str, Any],
        adload_version: Optional[str] = None,
        tail_number: Optional[str] = None
    ) -> "CampaignUpdate":
        resolved_version = adload_version or data.get("adload_version")
        if resolved_version is None:
            raise ValueError("adload_version must be provided or present in data")

        campaigns = [Campaign.from_dict(c) for c in data.get("campaigns", [])]

        creatives: List[Creative] = []
        for entry in data.get("creatives", []):
            for key, val in entry.items():
                creatives.append(Creative.from_dict(key, val))

        targeting_zones = data.get("targeting_zones", {})
        if not isinstance(targeting_zones, dict):
            raise ValueError("targeting_zones must be a dict of {zone_id: bool}")
        resolved_tail = tail_number or data.get("tail_number")

        return CampaignUpdate(
            adload_version=resolved_version,
            campaigns=campaigns,
            creatives=creatives,
            targeting_zones=targeting_zones,
            tail_number=resolved_tail,
        )


@dataclass
class RuleRecord:
    """
    Stored fields for COA Story 3 requirements:
      - rule_id          : unique identifier (16 hex chars)
      - payload_hash     : SHA-256 of the canonical payload (used for dedup lookup)
      - adload_version   : which adload this update belongs to
      - campaign_ids     : list of campaign IDs included in this update
      - tail_number      : which aircraft tail this was for (if known)
      - timestamp        : when this rule was first created
      - metadata         : the full CampaignUpdate payload for reference
    """
    rule_id: str
    payload_hash: str
    adload_version: str
    campaign_ids: List[int]
    timestamp: datetime
    metadata: Dict[str, Any]             
    tail_number: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "adload_version": self.adload_version,
            "campaign_ids": self.campaign_ids,
            "tail_number": self.tail_number,
            "timestamp": self.timestamp.isoformat() + "Z",
            "metadata": self.metadata,
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "RuleRecord":
        timestamp_raw = data.get("timestamp")
        if not isinstance(timestamp_raw, str):
            raise ValueError("timestamp must be a string in ISO 8601 format")

        ts = datetime.fromisoformat(timestamp_raw.rstrip("Z"))

        return RuleRecord(
            rule_id=data["rule_id"],
            payload_hash=data["payload_hash"],
            adload_version=data["adload_version"],
            campaign_ids=list(data.get("campaign_ids", [])),
            tail_number=data.get("tail_number"),
            timestamp=ts,
            metadata=data.get("metadata", {}),
        )
