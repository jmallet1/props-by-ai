// ParentComponent.js
import React from 'react';
import DonutChart from './DonutChart';
import './MatchupStats.css'

const ButtonsAndGraphs = ({ type, matchupDifficulty, matchup, avgLine, statPerGame }) => {

    const matchup_rank = matchupDifficulty.slice(0, -2);
    const matchup_rank_league_diff = 30 - matchup_rank;

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
                        <h3 className='DonutTitle'>{type.toUpperCase()} Per Game VS {matchup}</h3>
                    </div>
                    <div className='NumberContainer'>
                        <h1 className='Number'>{statPerGame}</h1>
                    </div>
                </div>
                <div className='DonutGraph'>
                    <DonutChart overData={statPerGame} underData={avgLine}/>
                </div>

            </div>
        </div>
    );
};

export default ButtonsAndGraphs;