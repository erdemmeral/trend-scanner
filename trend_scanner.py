# Modify the run_continuous_scan method
async def run_continuous_scan(self):
    """
    Run a single scan cycle and exit
    """
    try:
        cycle_start_time = datetime.now()
        logger.info(f"\n=== Starting Scan Cycle at {cycle_start_time.strftime('%Y-%m-%d %H:%M:%S')} ===")
        
        self.last_scan_time = cycle_start_time
        await self.initialize_components()
        
        # Run the category scanning
        results = await self.scan_all_categories()
        
        logger.info("\n=== Scan Cycle Complete ===")
        logger.info("Exiting program...")
        
        # Exit the program after completion
        sys.exit(0)
            
    except Exception as e:
        logger.error(f"Error in scan cycle: {str(e)}", exc_info=True)
        sys.exit(1) 