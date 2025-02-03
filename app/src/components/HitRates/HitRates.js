// ParentComponent.js
import React from 'react';
import AIPrediction from './AIPrediction'
import './HitRates.css';

function getColor(hr, szn_avg, avg_line){
    if(avg_line !== undefined)
        hr = hr.toString().replace("%","");
    if(hr === -1)
        return '#F9A825';
    else if(hr > 50 && avg_line !== undefined)
        return '#4caf50';
    else if(hr < 50 && avg_line !== undefined)
        return '#f44336';
    else if(hr > szn_avg && avg_line === undefined)
        return '#4caf50';
    else if(hr < szn_avg && avg_line === undefined)
        return '#f44336';    
    else
        return '#F9A825';
}

function getNumber(hr, avgLine){
    if(hr === 'N/A' || hr === -1)
        return -1;
    else if (avgLine !== undefined)
        return hr.replace("%","");
    else
        return 100
}


const HitRates = ({prediction, avgLine, type, HRData, propDate, windowWidth}) => {

    let h2h_hr = -1;
    let l5_hr = -1;
    let l20_hr = -1;
    let szn_hr = -1;

    for(let i=0; i < HRData.length; i++){
        
        let currentLabel = HRData[i][0];
        let currentPercentage = HRData[i][1];

        if(avgLine !== undefined)
            currentPercentage += '%';
        if(HRData[i][1] === -1){
            currentPercentage = 'N/A'
        }
        if(currentLabel === 'h2h'){
            h2h_hr = currentPercentage;
        } else if(currentLabel === 'last_5'){
            l5_hr = currentPercentage;
        } else if(currentLabel === 'last_20'){
            l20_hr = currentPercentage;
        } else if(currentLabel === 'szn'){
            szn_hr = currentPercentage;
        }
    }

    let h2h_number = getNumber(h2h_hr, avgLine);
    let l5_number = getNumber(l5_hr, avgLine);
    let l20_number = getNumber(l20_hr, avgLine);
    let szn_number = getNumber(szn_hr, avgLine);

    return (
        <div className='HRContainer'>
            <div className='aiPrediction'>
                <AIPrediction prediction={prediction} type={type} avgLine={avgLine} propDate={propDate} windowWidth={windowWidth}/>
            </div>
            <div className='hr'>
                <div className='initialColor' style={{ backgroundColor: getColor(h2h_hr, szn_hr, avgLine)}}></div>
                <div className='label'>H2H</div>
                <div className='bar'>
                    <div className='percentage' style={{ width: `${h2h_number === -1 ? 100 : h2h_number}%`, backgroundColor: getColor(h2h_hr, szn_hr, avgLine)}}></div>
                </div>
                <div className='percentageText'>{h2h_hr}</div>
            </div>
            <div className='hr'>
                <div className='initialColor' style={{ backgroundColor: getColor(l5_hr, szn_hr, avgLine)}}></div>
                <div className="label">L5</div>
                <div className='bar'>
                    <div className='percentage' style={{ width: `${l5_number === -1 ? 100 : l5_number}%`, backgroundColor: getColor(l5_hr, szn_hr, avgLine)}}></div>
                </div>
                <div className='percentageText'>{l5_hr}</div>
            </div>
            <div className='hr'>
                <div className='initialColor' style={{ backgroundColor: getColor(l20_hr, szn_hr, avgLine)}}></div>
                <div className='label'>L20</div>
                <div className='bar'>
                    <div className='percentage'  style={{ width: `${l20_number === -1 ? 100 : l20_number}%`, backgroundColor: getColor(l20_hr, szn_hr, avgLine)}}></div>
                </div>
                <div className='percentageText'>{l20_hr}</div>
            </div>
            <div className='hr'>
                <div className='initialColor' style={{ backgroundColor: getColor(szn_hr, szn_hr, avgLine)}}></div>
                <div className='label'>SZN</div>
                <div className='bar'>
                    <div className='percentage'  style={{ width: `${szn_number}%`, backgroundColor: getColor(szn_hr, szn_hr, avgLine)}}></div>
                </div>
                <div className='percentageText'>{szn_hr}</div>
            </div>


        </div>
    );
};

export default HitRates;