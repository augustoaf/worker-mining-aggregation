@echo off
echo Starting Worker Aggregation Application...
echo.

echo Building the project...
call mvn clean package -q

if %ERRORLEVEL% NEQ 0 (
    echo Build failed! Please check the errors above.
    pause
    exit /b 1
)

echo.
echo Starting the aggregation pipeline...
echo.
echo Make sure RabbitMQ is running on localhost:5672
echo The application will:
echo - Read from queue: raw_equipment_events
echo - Aggregate temperature data over 10-second windows
echo - Write results to queue: agg_temperature
echo.
echo Press Ctrl+C to stop the application
echo.

java -jar target/worker-aggregation-1.0.0.jar

pause 