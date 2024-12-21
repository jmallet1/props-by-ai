// ParentComponent.js
import React from 'react';
import AIPrediction from './AIPrediction'
import './HitRates.css';

function getColor(hr){
    if(hr > 50)
        return '#4caf50';
    else if(hr == -1)
        return '#F9A825';
    else if(hr < 50)
        return '#f44336';
    else
        return '#F9A825';
}

function getNumber(hr){
    if(hr == 'N/A' || hr == -1)
        return -1;
    else
        return hr.replace("%","");
}


const HitRates = ({prediction, avgLine, type, HRData}) => {

    let h2h_hr = -1;
    let l5_hr = -1;
    let l20_hr = -1;
    let szn_hr = -1;

    for(let i=0; i < HRData.length; i++){
        let currentLabel = HRData[i][0];
        let currentPercentage = HRData[i][1] + '%';
        if(HRData[i][1] == -1){
            currentPercentage = 'N/A'
        }
        if(currentLabel == 'h2h'){
            h2h_hr = currentPercentage;
        } else if(currentLabel == 'last_5'){
            l5_hr = currentPercentage;
        } else if(currentLabel == 'last_20'){
            l20_hr = currentPercentage;
        } else if(currentLabel == 'szn'){
            szn_hr = currentPercentage;
        }
    }

    const h2h_number = getNumber(h2h_hr);
    const l5_number = getNumber(l5_hr);
    const l20_number = getNumber(l20_hr);
    const szn_number = getNumber(szn_hr);


    return (
        <div className='HRContainer'>
            <div className='aiPrediction'>
                <AIPrediction prediction={prediction} type={type} avgLine={avgLine} />
            </div>
            <div className='hr'>
                <div className='initialColor' style={{ backgroundColor: getColor(h2h_number)}}></div>
                <div className='label'>H2H</div>
                <div className='bar'>
                    <div className='percentage' style={{ width: `${h2h_number == -1 ? 100 : h2h_number}%`, backgroundColor: getColor(h2h_number)}}></div>
                </div>
                <div className='percentageText'>{h2h_hr}</div>
            </div>
            <div className='hr'>
                <div className='initialColor' style={{ backgroundColor: getColor(l5_number)}}></div>
                <div className="label">L5</div>
                <div className='bar'>
                    <div className='percentage' style={{ width: `${l5_number}%`, backgroundColor: getColor(l5_number)}}></div>
                </div>
                <div className='percentageText'>{l5_hr}</div>
            </div>
            <div className='hr'>
                <div className='initialColor' style={{ backgroundColor: getColor(l20_number)}}></div>
                <div className='label'>L20</div>
                <div className='bar'>
                    <div className='percentage'  style={{ width: `${l20_number == -1 ? 100 : l20_number}%`, backgroundColor: getColor(l20_number)}}></div>
                </div>
                <div className='percentageText'>{l20_hr}</div>
            </div>
            <div className='hr'>
                <div className='initialColor' style={{ backgroundColor: getColor(szn_number)}}></div>
                <div className='label'>SZN</div>
                <div className='bar'>
                    <div className='percentage'  style={{ width: `${szn_number}%`, backgroundColor: getColor(szn_number)}}></div>
                </div>
                <div className='percentageText'>{szn_hr}</div>
            </div>


        </div>
    );
};

export default HitRates;