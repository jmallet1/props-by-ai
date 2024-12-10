// ParentComponent.js
import React from 'react';
import './MatchupInfo.css'

export const MatchupInfo = () => {
    return (
        <>
            <div className='MatchupBox'>
                    <div className='MatchupChild'>
                        <h1>vs. CLE</h1>
                        <p>Opponent</p>
                    </div>
                    <div className='MatchupChild'>                        
                        <h1>22.5</h1>
                        <p>PPG vs CLE</p>
                    </div>
                    <div className='MatchupChild'>
                        <h1>13th</h1>
                        <p>Matchup Rank</p>
                    </div>
            </div>
        </>
    );
};

export default MatchupInfo;