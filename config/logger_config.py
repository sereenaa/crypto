
import logging
import watchtower

# Configure logging
log_group_name = 'arb-opcodes'  

cloudwatch_handler = watchtower.CloudWatchLogHandler(log_group=log_group_name, 
                                                     send_interval=30,   # Send logs every 30 seconds
                                                     max_batch_count=50, # Maximum number of logs per batch
                                                    )  # Optional stream name

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        cloudwatch_handler,
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)