// ParentComponent.js
import React from 'react';
import { useAuth } from "react-oidc-context";
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

const AIPrediction = ({prediction, type, avgLine, propDate, windowWidth}) => {
    const auth = useAuth();

    const predictionGraphics = getIcon(prediction, avgLine);
    let fontSizePrediction;
    let fontSizeType;
    if(avgLine === undefined){
        fontSizePrediction = "clamp(5rem, 12vw, 6.5rem)";
        fontSizeType =  "clamp(1rem, 5vw, 3rem)";
    } else {
        fontSizePrediction = "clamp(3rem, 8vw, 4.5rem)";
        fontSizeType =  "clamp(1rem, 3vw, 2rem)";
    }

    return (
        <div className='predictionContainer'>
            <h3 className='title'>AI Prediction - {propDate}</h3>
            {auth.isAuthenticated ? (
                <>
                    <div className='outputContainer'>
                        <h1 className='prediction' style={{ fontSize: fontSizePrediction }}>{prediction}</h1>
                        <p style={{ fontSize: fontSizeType }}>{type.toUpperCase()}</p>
                    </div>
                    {avgLine !== undefined && (
                        <div className='decision'>
                            <img className='decisionIcon' src={predictionGraphics.icon} alt="over-under"/>
                            <p>{predictionGraphics.text}</p>
                        </div>
                    )}
                </>
            )  : (
                // Show blurred-out numbers with login prompt if not authenticated
                <div className="blurredWrapper">
                    <h1 className="prediction blurredText">15.2</h1>
                    <button onClick={() => auth.signinRedirect()} className='loginMessage'>Login or Sign Up to View</button>
                </div>
            )}
            {windowWidth > 600 &&
                <div className='logoIconContainer'>
                    <img src={logo} className='logoIcon' alt="pba-logo"/>
                </div>
            }       
        </div>
    );
};

export default AIPrediction;