import React from 'react';
import './AvgLine.css'
import arrow from '../../assets/pictures/arrow.png'

const AvgLine = ({ line, type}) => {

    const line_text = "Line --> " + line + " " + type.toUpperCase();

    return (
        <div className='LineContainer'>
            <img src={arrow} alt='Arrow'/>
            <h1>{line_text}</h1>
        </div>
    );
};

export default AvgLine;