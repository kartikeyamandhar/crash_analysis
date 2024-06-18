import os
import json
import logging
from analysis import CrashAnalysis

def perform_analyses(spark, config, primary_person_df, units_df, damages_df, charges_df):
    analysis = CrashAnalysis(spark)

    try:
        result1 = analysis.analysis_1(primary_person_df)
        result2 = analysis.analysis_2(units_df)
        result3 = analysis.analysis_3(units_df, primary_person_df)
        result4 = analysis.analysis_4(units_df, primary_person_df)
        result5 = analysis.analysis_5(primary_person_df)
        result6 = analysis.analysis_6(units_df)
        result7 = analysis.analysis_7(primary_person_df, units_df)
        result8 = analysis.analysis_8(primary_person_df, units_df)
        result9 = analysis.analysis_9(units_df, damages_df)
        result10 = analysis.analysis_10(units_df, primary_person_df, charges_df)
    except Exception as e:
        logging.error(f"Error performing analysis: {e}")
        raise

    results = {
        "Analysis 1 result": result1,
        "Analysis 2 result": result2,
        "Analysis 3 result": [row.asDict() for row in result3],
        "Analysis 4 result": result4,
        "Analysis 5 result": result5,
        "Analysis 6 result": result6,
        "Analysis 7 result": result7,
        "Analysis 8 result": result8,
        "Analysis 9 result": result9,
        "Analysis 10 result": [row.asDict() for row in result10]
    }

    # Output all results to one file
    try:
        output_path = os.path.join(config['output_path'], 'analysis_results.json')
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=4)
        logging.info("All results written successfully.")
    except Exception as e:
        logging.error(f"Error writing output: {e}")
        raise
