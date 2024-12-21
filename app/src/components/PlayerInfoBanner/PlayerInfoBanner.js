import './PlayerInfoBanner.css';
import playerPic from '../../assets/pictures/fvv.png';
import React, { useState, useEffect } from 'react';

const PlayerInfoBanner = ({matchup, playerInfo, seasonAvg, playerId}) => {

    const img_url = `https://cdn.nba.com/headshots/nba/latest/1040x760/${playerId}.png`;

    const playerName = playerInfo.player_name;
    // Split the name into two parts
    const nameParts = playerName.split(' ');

    return (
          <div className='PlayerInfoContainer'>
            <div className='PlayerPicAndName'>
              <div className='PlayerHeadshot'>
                <img src={img_url} />
              </div>
              <div className="PlayerDetails">
                  <div className='MatchupInfo'>
                      {   
                          nameParts.length >= 2 ? (
                              <>
                                  <div className={"playerName"}>{nameParts[0]}</div>
                                  <div className={"playerName"}>{nameParts[1]}</div>
                              </>
                          ) : (
                              <div>{playerName}</div> // In case there's only one name part
                          )
                      }
                        <p>
                          {playerInfo.position} - {playerInfo.team} vs {matchup}
                        </p>
                  </div>
              </div>
            </div>
            <div className='SeasonAvgLarge'>
              <div className='SeasonAvgSmall'>
                <div className='Stat'>
                  <h3>{seasonAvg.pts}</h3>
                  <p>PPG</p></div>
                <div className="StatSeparator"></div>
                <div className='Stat'>
                  <h3>{seasonAvg.reb}</h3>
                  <p>RPG</p>
                </div>
                <div className="StatSeparator"></div>
                <div className='Stat'>
                  <h3>{seasonAvg.ast}</h3>
                  <p>APG</p>
                </div>
              </div>
            </div>
          </div>
    );
};

export default PlayerInfoBanner;