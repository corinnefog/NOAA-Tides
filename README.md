#NOAA Tides Data Collection Pipeline

This application pulls from the NOAA Tides database that updates every 6 minutes with observed tides data, and verifies that data hours after. This project pulls from 6 hours previous to the time the run is triggered to collect verified data only. It is then compared to what NOAA predicted the tide level would be at that time. The data is from Sewells Point which is the station collecting data in the Virginia Beach/Norfolk area. This was chosen because it is near my hometown.

The steps of this app goes as follows:
1. Takes datetime and rounds it to the near 6 minute mark to match NOAA data
2. Fetches data from NOAA API and downloads the tide data
3. Picks out the exact reading from NOAA for the time
4. Calculate the difference between predicted and observed tide levels and categprizes it as stable, falling, rising, or surge
5. Tide label is saved in a DynamoDB table
6. All past results from the table are then loaded
7. The plot is updated with the new data
8. The plot and updated CSV are then loaded to S3

The output data and plot is fetched data from every hour on the hour and the tide level for both the predicted and observed levels side by side. In the graph, you can see the natural rise and fall of the tides as time progresses. The CSV file has both the observed level and predicted level stored, as well as the difference between those two, the collection timestamp, the classification label, and the station ID from Sewewlls Point
