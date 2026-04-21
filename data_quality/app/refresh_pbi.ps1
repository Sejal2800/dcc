# Path to the Power BI Desktop file
$pbixFilePath = ""C:\Users\Aryan Sachan\Downloads\DATA QUALITY DASH.pbix""

# Start Power BI Desktop and open the report
Start-Process -FilePath "C:\Program Files\Microsoft Power BI Desktop\bin\PBIDesktop.exe" -ArgumentList "`"$pbixFilePath`""
#"C:\Program Files\Microsoft Power BI Desktop\bin\PBIDesktop.exe"
# Wait for Power BI Desktop to load (adjust timing if needed)
Start-Sleep -Seconds 15

# Refresh the report
$xlApp = New-Object -ComObject Excel.Application
$xlWorkbook = $xlApp.Workbooks.Open($pbixFilePath)
$xlWorkbook.RefreshAll()
$xlWorkbook.Save()
$xlWorkbook.Close()
$xlApp.Quit()

Write-Output "Power BI report refreshed successfully."