cd "../"
# Exit immediately if a command fails
set -e

# Define the output directory
OUTPUT_DIR="../output/"

# Run for specific word counts
echo "Running specific word counts..."
python src/run.py process_data -cfg config/config.yaml -dataset "sh0416/ag_news" -dirout "$OUTPUT_DIR"

# Run for all word counts
echo "Running all word counts..."
python src/run.py process_data_all -cfg config/config.yaml -dataset "sh0416/ag_news" -dirout "$OUTPUT_DIR"


echo "Running Unit Tests..."
pytest src/test_run.py
echo "Processing completed successfully!"

