import os
import sys
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_analysis():
    try:
        logger.info("Starting wallet analysis pipeline...")
        
        # Step 1: Run wallet holdings analyzer
        logger.info("Running wallet holdings analyzer...")
        import wallet_holdings_analyzer
        wallet_holdings_analyzer.main()
        
        # Step 2: Run wallet score manager
        logger.info("Running wallet score manager...")
        import wallet_score_manager
        wallet_score_manager.main()
        
        logger.info("Analysis pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in analysis pipeline: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run_analysis() 