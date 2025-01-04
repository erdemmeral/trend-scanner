async def main():
    """Main application entry point with error recovery"""
    logger.info("=== Tech Trend Scanner Starting ===")
    logger.info(f"Current time: {datetime.now()}")
    logger.info("Checking environment variables:")
    logger.info(f"TELEGRAM_BOT_TOKEN set: {'Yes' if TELEGRAM_BOT_TOKEN else 'No'}")
    logger.info(f"TELEGRAM_CHAT_IDS set: {'Yes' if TELEGRAM_CHAT_IDS else 'No'}")
    logger.info(f"SCAN_INTERVAL_HOURS: {SCAN_INTERVAL_HOURS}")
    log_memory()
    
    try:
        logger.info("Initializing scanner...")
        scanner = TrendScanner()
        logger.info("Starting application...")
        await scanner.start_app()
        logger.info("Running scan...")
        await scanner.run_continuous_scan()
    except Exception as e:
        logger.error(f"Critical error: {str(e)}", exc_info=True)
        raise 