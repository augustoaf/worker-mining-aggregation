@echo off
echo Worker Aggregation Test Script
echo.

echo Choose an option:
echo 1. Run Test Producer (sends sample events to raw_equipment_events queue)
echo 2. Run Test Consumer (reads aggregated results from agg_temperature queue)
echo 3. Run Main Aggregation Pipeline
echo 4. Build project only
echo.

set /p choice="Enter your choice (1-4): "

if "%choice%"=="1" (
    echo.
    echo Starting Test Producer...
    echo This will send sample equipment events to the raw_equipment_events queue
    echo.
    mvn exec:java -Dexec.mainClass="com.equipment.aggregation.TestProducer"
) else if "%choice%"=="2" (
    echo.
    echo Starting Test Consumer...
    echo This will read aggregated results from the agg_temperature queue
    echo.
    mvn exec:java -Dexec.mainClass="com.equipment.aggregation.TestConsumer"
) else if "%choice%"=="3" (
    echo.
    echo Starting Main Aggregation Pipeline...
    echo.
    mvn exec:java -Dexec.mainClass="com.equipment.aggregation.TemperatureAggregationJob"
) else if "%choice%"=="4" (
    echo.
    echo Building project...
    mvn clean package
) else (
    echo Invalid choice. Please run the script again and select 1-4.
)

pause 