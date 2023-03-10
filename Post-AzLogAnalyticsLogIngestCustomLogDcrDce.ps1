Function Post-AzLogAnalyticsLogIngestCustomLogDcrDce
{
 <#
    .SYNOPSIS
    Send data to LogAnalytics using Log Ingestion API and Data Collection Rule

    .DESCRIPTION
    Data is either sent as one record (if only one exist), batches (calculated value of number of records to send per batch)
    - or BatchAmount (used only if the size of the records changes so you run into problems with limitations. 
    In case of diffent sizes, use 1 for BatchAmount
    Sending data in UTF8 format

    .PARAMETER DceUri
    Here you can put in the DCE uri - typically found using Get-DceDcrDetails

    .PARAMETER DcrImmutableId
    Here you can put in the DCR ImmunetableId - typically found using Get-DceDcrDetails

    .PARAMETER DcrStream
    Here you can put in the DCR Stream name - typically found using Get-DceDcrDetails

    .PARAMETER Data
    This is the data array

    .PARAMETER AzAppId
    This is the Azure app id og an app with Contributor permissions in LogAnalytics + Resource Group for DCRs
        
    .PARAMETER AzAppSecret
    This is the secret of the Azure app

    .PARAMETER TenantId
    This is the Azure AD tenant id

    .INPUTS
    None. You cannot pipe objects

    .OUTPUTS
    Output of REST PUT command. Should be 200 for success

    .EXAMPLE
	$AzDcrDceDetails = Get-AzDcrDceDetails -DcrName $DcrName -DceName $DceName `
										   -AzAppId $AzAppId -AzAppSecret $AzAppSecret -TenantId $TenantId -Verbose:$Verbose

	Post-AzLogAnalyticsLogIngestCustomLogDcrDce  -DceUri $AzDcrDceDetails[2] -DcrImmutableId $AzDcrDceDetails[6] -TableName $TableName `
												 -DcrStream $AzDcrDceDetails[7] -Data $Data -BatchAmount $BatchAmount `
												 -AzAppId $AzAppId -AzAppSecret $AzAppSecret -TenantId $TenantId -Verbose:$Verbose

 #>

    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [string]$DceURI,
            [Parameter(mandatory)]
                [string]$DcrImmutableId,
            [Parameter(mandatory)]
                [string]$DcrStream,
            [Parameter(mandatory)]
                [Array]$Data,
            [Parameter(mandatory)]
                [string]$TableName,
            [Parameter()]
                [string]$BatchAmount,
            [Parameter()]
                [string]$AzAppId,
            [Parameter()]
                [string]$AzAppSecret,
            [Parameter()]
                [string]$TenantId
         )

    #--------------------------------------------------------------------------
    # Data check
    #--------------------------------------------------------------------------

        If ($DceURI -and $DcrImmutableId -and $DcrStream -and $Data)
            {
                # Add assembly to upload using http
                Add-Type -AssemblyName System.Web

                #--------------------------------------------------------------------------
                # Obtain a bearer token used to authenticate against the data collection endpoint using Azure App & Secret
                #--------------------------------------------------------------------------

                    $scope       = [System.Web.HttpUtility]::UrlEncode("https://monitor.azure.com//.default")   
                    $bodytoken   = "client_id=$AzAppId&scope=$scope&client_secret=$AzAppSecret&grant_type=client_credentials";
                    $headers     = @{"Content-Type"="application/x-www-form-urlencoded"};
                    $uri         = "https://login.microsoftonline.com/$tenantId/oauth2/v2.0/token"

                    $bearerToken = (Invoke-RestMethod -Uri $uri -Method "Post" -Body $bodytoken -Headers $headers).access_token

                    $headers = @{
                                    "Authorization" = "Bearer $bearerToken";
                                    "Content-Type" = "application/json";
                                }


                #--------------------------------------------------------------------------
                # Upload the data using Log Ingesion API using DCE/DCR
                #--------------------------------------------------------------------------
                    
                    # initial variable
                    $indexLoopFrom = 0

                    # calculate size of data (entries)
                    $TotalDataLines = ($Data | Measure-Object).count

                    # calculate number of entries to send during each transfer - log ingestion api limits to max 1 mb per transfer
                    If ( ($TotalDataLines -gt 1) -and ($BatchAmount -eq $null) )
                        {
                            $SizeDataSingleEntryJson  = (ConvertTo-Json -Depth 100 -InputObject @($Data[0]) -Compress).length
                            $DataSendAmountDecimal    = (( 1mb - 300Kb) / $SizeDataSingleEntryJson)   # 500 Kb is overhead (my experience !)
                            $DataSendAmount           = [math]::Floor($DataSendAmountDecimal)
                        }
                    ElseIf ($BatchAmount)
                        {
                            $DataSendAmount           = $BatchAmount
                        }
                    Else
                        {
                            $DataSendAmount           = 1
                        }

                    # loop - upload data in batches, depending on possible size & Azure limits 
                    Do
                        {
                            $DataSendRemaining = $TotalDataLines - $indexLoopFrom

                            If ($DataSendRemaining -le $DataSendAmount)
                                {
                                    # send last batch - or whole batch
                                    $indexLoopTo    = $TotalDataLines - 1   # cause we start at 0 (zero) as first record
                                    $DataScopedSize = $Data   # no need to split up in batches
                                }
                            ElseIf ($DataSendRemaining -gt $DataSendAmount)
                                {
                                    # data must be splitted in batches
                                    $indexLoopTo    = $indexLoopFrom + $DataSendAmount
                                    $DataScopedSize = $Data[$indexLoopFrom..$indexLoopTo]
                                }

                            # Convert data into JSON-format
                            $JSON = ConvertTo-Json -Depth 100 -InputObject @($DataScopedSize) -Compress

                            If ($DataSendRemaining -gt 1)    # batch
                                {
                                    write-Output ""
                                    
                                    # we are showing as first record is 1, but actually is is in record 0 - but we change it for gui purpose
                                    Write-Output "  [ $($indexLoopFrom + 1)..$($indexLoopTo + 1) / $($TotalDataLines) ] - Posting data to Loganalytics table [ $($TableName)_CL ] .... Please Wait !"
                                }
                            ElseIf ($DataSendRemaining -eq 1)   # single record
                                {
                                    write-Output ""
                                    Write-Output "  [ $($indexLoopFrom + 1) / $($TotalDataLines) ] - Posting data to Loganalytics table [ $($TableName)_CL ] .... Please Wait !"
                                }

                            $uri = "$DceURI/dataCollectionRules/$DcrImmutableId/streams/$DcrStream"+"?api-version=2021-11-01-preview"
                            
                            # set encoding to UTF8
                            $JSON = [System.Text.Encoding]::UTF8.GetBytes($JSON)

                            $Result = Invoke-WebRequest -Uri $uri -Method POST -Body $JSON -Headers $headers -ErrorAction SilentlyContinue
                            $StatusCode = $Result.StatusCode

                            If ($StatusCode -eq "204")
                                {
                                    Write-host "  SUCCESS - data uploaded to LogAnalytics"
                                }
                            ElseIf ($StatusCode -eq "RequestEntityTooLarge")
                                {
                                    Write-Error "  Error 513 - You are sending too large data - make the dataset smaller"
                                }
                            Else
                                {
                                    Write-Error $result
                                }

                            # Set new Fom number, based on last record sent
                            $indexLoopFrom = $indexLoopTo

                        }
                    Until ($IndexLoopTo -ge ($TotalDataLines - 1 ))
            
              # return $result
        }
        Write-host ""
}
