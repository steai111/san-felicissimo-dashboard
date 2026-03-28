from dataclasses import dataclass
from typing import Optional


@dataclass
class DashboardMetric:
    metric_key: str
    metric_label: str
    period_start: str
    period_end: str
    metric_value: str
    source_type: str
    source_detail: Optional[str] = None


@dataclass
class BeddyChannelStat:
    period_start: str
    period_end: str
    channel_name: str
    total_bookings: Optional[int] = None
    nights_sold: Optional[float] = None
    average_stay: Optional[float] = None
    arrivals: Optional[int] = None
    departures: Optional[int] = None
    revenue: Optional[float] = None
    occupancy_rate: Optional[float] = None
    incidence_rate: Optional[float] = None
    source_file: Optional[str] = None


@dataclass
class BeddyNationalityStat:
    period_start: str
    period_end: str
    nationality: str
    nights_sold: Optional[float] = None
    total_bookings: Optional[int] = None
    average_stay: Optional[float] = None
    arrivals: Optional[int] = None
    departures: Optional[int] = None
    source_file: Optional[str] = None


@dataclass
class TableauReservation:
    unit_name: str
    source_day: str
    reservation_id: Optional[str] = None
    guest_name: Optional[str] = None
    check_in: Optional[str] = None
    check_out: Optional[str] = None
    nights: Optional[int] = None
    adults: Optional[int] = None
    children: Optional[int] = None
    source_file: Optional[str] = None