Function Build-DataArrayToAlignWithSchema
{
 <#
    .SYNOPSIS
    Rebuilds the source object to match modified schema structure - used after usage of ValidateFix-AzLogAnalyticsTableSchemaColumnNames

    .DESCRIPTION
    Builds new PSCustomObject object

    .PARAMETER Data
    This is the data array

    .INPUTS
    None. You cannot pipe objects

    .OUTPUTS
    Updated $DataVariable with valid column names

    .EXAMPLE
    PS> $DataVariable = Build-DataArrayToAlignWithSchema -Data $DataVariable -Verbose:$Verbose

 #>

    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [Array]$Data
         )

    Write-Verbose "  Aligning source object structure with schema ... Please Wait !"
    
    # Get schema
    $Schema = Get-ObjectSchemaAsArray -Data $Data -Verbose:$Verbose

    $DataCount  = ($Data | Measure-Object).Count

    $DataVariableQA = @()

    $Data | ForEach-Object -Begin  {
            $i = 0
    } -Process {
                    # get column names
                  #  $ObjColumns = $_ | Get-Member -MemberType NoteProperty

                    # enum schema
                    ForEach ($Column in $Schema)
                        {
                            # get column name & data
                            $ColumnName = $Column.Name
                            $ColumnData = $_.$ColumnName

                            $_ | Add-Member -MemberType NoteProperty -Name $ColumnName -Value $ColumnData -Force
                        }
                    $DataVariableQA += $_

                    # Increment the $i counter variable which is used to create the progress bar.
                    $i = $i+1

                    # Determine the completion percentage
                    $Completed = ($i/$DataCount) * 100
                    Write-Progress -Activity "Aligning source object structure with schema" -Status "Progress:" -PercentComplete $Completed
            } -End {
                
                Write-Progress -Activity "Aligning source object structure with schema" -Status "Ready" -Completed
                # return data from temporary array to original $Data
                $Data = $DataVariableQA
            }
        Return $Data
}
