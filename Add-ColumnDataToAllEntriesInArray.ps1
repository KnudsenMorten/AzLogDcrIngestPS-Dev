Function Add-ColumnDataToAllEntriesInArray
{
    [CmdletBinding()]
    param(
            [Parameter(mandatory)]
                [Array]$Data,
            [Parameter(mandatory)]
                [string]$Column1Name,
            [Parameter(mandatory)]
                [string]$Column1Data,
            [Parameter()]
                [string]$Column2Name,
            [Parameter()]
                [string]$Column2Data,
            [Parameter()]
                [string]$Column3Name,
            [Parameter()]
                [string]$Column3Data
         )

    Write-Verbose "  Adding columns to all entries in array .... please wait !"
    $IntermediateObj = @()
    ForEach ($Entry in $Data)
        {
            If ($Column1Name)
                {
                    $Entry | Add-Member -MemberType NoteProperty -Name $Column1Name -Value $Column1Data -Force
                }

            If ($Column2Name)
                {
                    $Entry | Add-Member -MemberType NoteProperty -Name $Column2Name -Value $Column2Data -Force
                }

            If ($Column3Name)
                {
                    $Entry | Add-Member -MemberType NoteProperty -Name $Column3Name -Value $Column3Data -Force
                }

            $IntermediateObj += $Entry
        }
    return [array]$IntermediateObj
}
