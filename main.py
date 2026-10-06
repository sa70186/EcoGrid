import logging
from datetime import datetime

from ecogrid.marketplace.event_handlers import MeterReadingHandler
from ecogrid.marketplace.models import BuyRequest
from ecogrid.marketplace.service import MarketplaceService
from ecogrid.meter_ingestion.ingestion import MeterIngestionService
from ecogrid.meter_ingestion.models import MeterReading
from ecogrid.meter_ingestion.publisher import ReadingPublisher
from ecogrid.settlement.event_handlers import TradeMatchedHandler
from ecogrid.settlement.ledger import Ledger
from ecogrid.settlement.service import SettlementService
from ecogrid.shared.event_bus import EventBus
from ecogrid.shared.events import MeterReadingReceived, TradeMatched

DEFAULT_PRICE_PER_KWH = 0.20


def build_system():
    """Create the three contexts and connect them through the event bus."""
    event_bus = EventBus()

    # Financial Settlement
    ledger = Ledger()
    settlement_service = SettlementService(ledger)
    event_bus.subscribe(TradeMatched, TradeMatchedHandler(settlement_service).handle)

    # Marketplace
    marketplace = MarketplaceService(event_bus)
    event_bus.subscribe(
        MeterReadingReceived,
        MeterReadingHandler(marketplace, DEFAULT_PRICE_PER_KWH).handle,
    )

    # Smart Meter Integration
    ingestion = MeterIngestionService(ReadingPublisher(event_bus))

    return ingestion, marketplace, ledger


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")

    ingestion, marketplace, ledger = build_system()
    now = datetime.now()

    # Smart meters send readings
    ingestion.ingest(MeterReading("meter-1", "house-1", 6.0, 2.0, now))
    ingestion.ingest(MeterReading("meter-2", "house-2", 3.0, 1.0, now))
    ingestion.ingest(MeterReading("meter-3", "house-3", 2.0, -1.0, now))

    # A neighbour wants to buy energy
    trades = marketplace.submit_request(BuyRequest("house-4", 5.0, 0.25))

    print("\n--- Summary ---")
    print(f"Readings accepted: {ingestion.accepted_count}")
    print(f"Readings rejected: {ingestion.rejected_count}")
    print(f"Trades made: {len(trades)}")
    for settlement in ledger.all():
        print(
            f"Settlement: {settlement.payer_id} pays {settlement.payee_id} "
            f"{settlement.amount:.2f} ({settlement.status.value})"
        )


if __name__ == "__main__":
    main()