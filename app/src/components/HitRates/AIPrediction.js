// ParentComponent.js
import React from 'react';
import './AIPrediction.css';
import logo from '../../assets/pictures/logo.png'
import overIcon from '../../assets/pictures/over_icon.png'
import underIcon from '../../assets/pictures/under_icon.png'
import pushIcon from '../../assets/pictures/push_icon.png'

function getIcon(pred, line){
    if(pred > line)
        return {icon: overIcon, text: 'Over'};
    else if(pred < line)
        return {icon: underIcon, text: 'Under'};
    else
    return {icon: pushIcon, text: 'Push'};
}

const AIPrediction = ({prediction, type, avgLine}) => {

    const predictionGraphics = getIcon(prediction, avgLine);

    return (
        <div className='predictionContainer'>
            <h3 className='title'>AI Prediction</h3>
            <div className='outputContainer'>
                <h1 className='prediction'>{prediction}</h1>
                <p>{type.toUpperCase()}</p>
            </div>
            <div className='decision'>
                <img className='decisionIcon' src={predictionGraphics.icon} alt="over-under"/>
                <p>{predictionGraphics.text}</p>
            </div>
            <div className='logoIconContainer'>
                <img src={logo} className='logoIcon' alt="pba-logo"/>
            </div>
        </div>
    );
};

export default AIPrediction;