// ParentComponent.js
import React from 'react';
import DonutChart from './DonutChart';
import './MatchupStats.css'

const ButtonsAndGraphs = ({ type, matchupDifficulty, matchup, prediction, statPerGame }) => {

    const matchup_rank = matchupDifficulty.slice(0, -2);
    const matchup_rank_league_diff = 30 - matchup_rank;

    console.log(statPerGame);
    console.log(prediction)

    return (
        <div className='DonutContainer'>
            <div className='ModernDonut'>
                <div className='DonutTextContainer'>
                    <div className='DonutTitleContainer'>
                        <h3 className='DonutTitle'>Matchup Difficulty</h3>
                    </div>
                    <div className='NumberContainer'>
                        <h1 className='Number'>{matchupDifficulty}</h1>
                    </div>
                </div>
                <div className='DonutGraph'>
                    <DonutChart overData={matchup_rank} underData={matchup_rank_league_diff} />
                </div>
            </div>
            <div className='ModernDonut'>
                <div className='DonutTextContainer'>
                    <div className='DonutTitleContainer'>
                        <h3 className='DonutTitle'>{type.toUpperCase()} Per Game vs {matchup}</h3>
                    </div>
                    <div className='NumberContainer'>
                        <h1 className='Number'>{statPerGame === -1 ? 'N/A' : statPerGame}</h1>
                    </div>
                </div>
                <div className='DonutGraph'>
                    <DonutChart overData={statPerGame === -1 ? 0.1 : statPerGame} underData={statPerGame === 0 && prediction == 0 ? 1 : prediction}/>
                </div>

            </div>
        </div>
    );
};

export default ButtonsAndGraphs;